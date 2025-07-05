from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
import pandas as pd
import numpy as np
from typing import List, Dict, Any
import os
import re
from pathlib import Path

# Initialize FastAPI app
app = FastAPI(
    title="Excel Processing API",
    description="API for processing Excel sheets and extracting table data",
    version="1.0.0"
)

# Global variable to store Excel data
excel_data = {}
EXCEL_FILE_PATH = "capbudg.xls"

class ExcelProcessor:
    """Class to handle Excel file processing and data extraction"""
    
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.sheets_data = {}
        self.tables = {}
        
    def load_excel_file(self):
        """Load Excel file and read all sheets"""
        try:
            if not os.path.exists(self.file_path):
                raise FileNotFoundError(f"Excel file not found at {self.file_path}")
            
            # Read all sheets from Excel file
            excel_file = pd.ExcelFile(self.file_path)
            
            for sheet_name in excel_file.sheet_names:
                df = pd.read_excel(self.file_path, sheet_name=sheet_name, header=None)
                self.sheets_data[sheet_name] = df
                
            self._identify_tables()
            
        except Exception as e:
            raise Exception(f"Error loading Excel file: {str(e)}")
    
    def _identify_tables(self):
        """Identify tables in the Excel sheets"""
        for sheet_name, df in self.sheets_data.items():
            tables_in_sheet = self._extract_tables_from_sheet(df, sheet_name)
            self.tables.update(tables_in_sheet)
    
    def _extract_tables_from_sheet(self, df: pd.DataFrame, sheet_name: str) -> Dict[str, pd.DataFrame]:
        """Extract tables from a sheet based on patterns and structure"""
        tables = {}
        
        # Strategy 1: Look for table headers (bold text, merged cells indicators, etc.)
        # Strategy 2: Look for patterns like empty rows separating tables
        # Strategy 3: Look for specific keywords that indicate table starts
        
        current_table = None
        current_table_name = None
        current_table_data = []
        
        for idx, row in df.iterrows():
            # Convert row to string and check for table indicators
            row_str = ' '.join([str(cell) for cell in row.dropna()])
            
            # Check if this row might be a table header
            if self._is_potential_table_header(row_str):
                # Save previous table if exists
                if current_table_name and current_table_data:
                    table_df = pd.DataFrame(current_table_data)
                    tables[current_table_name] = table_df
                
                # Start new table
                current_table_name = row_str.strip()
                current_table_data = []
                
            elif current_table_name and not row.isna().all():
                # Add row to current table
                current_table_data.append(row.tolist())
        
        # Save last table
        if current_table_name and current_table_data:
            table_df = pd.DataFrame(current_table_data)
            tables[current_table_name] = table_df
        
        # If no clear table structure found, treat entire sheet as one table
        if not tables:
            # Look for meaningful data sections
            non_empty_rows = df.dropna(how='all')
            if not non_empty_rows.empty:
                # Try to identify sections based on content
                tables[f"{sheet_name}_data"] = non_empty_rows
        
        return tables
    
    def _is_potential_table_header(self, text: str) -> bool:
        """Check if a text string could be a table header"""
        if not text or text.strip() == '':
            return False
        
        # Common table header indicators
        header_keywords = [
            'investment', 'revenue', 'expense', 'projection', 'cash flow',
            'initial', 'operating', 'capital', 'budget', 'financial'
        ]
        
        text_lower = text.lower()
        
        # Check for header keywords
        for keyword in header_keywords:
            if keyword in text_lower:
                return True
        
        # Check for patterns that suggest headers
        if len(text.split()) <= 5 and len(text) > 5:  # Short descriptive text
            return True
        
        return False
    
    def get_table_names(self) -> List[str]:
        """Get list of all table names"""
        return list(self.tables.keys())
    
    def get_table_row_names(self, table_name: str) -> List[str]:
        """Get row names (first column values) for a specific table"""
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' not found")
        
        table_df = self.tables[table_name]
        if table_df.empty:
            return []
        
        # Get first column values, excluding empty cells
        first_column = table_df.iloc[:, 0]
        row_names = []
        
        for value in first_column:
            if pd.notna(value) and str(value).strip():
                row_names.append(str(value).strip())
        
        return row_names
    
    def calculate_row_sum(self, table_name: str, row_name: str) -> float:
        """Calculate sum of numerical values in a specific row"""
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' not found")
        
        table_df = self.tables[table_name]
        
        # Find the row with matching name in first column
        target_row = None
        for idx, row in table_df.iterrows():
            if pd.notna(row.iloc[0]) and str(row.iloc[0]).strip() == row_name:
                target_row = row
                break
        
        if target_row is None:
            raise ValueError(f"Row '{row_name}' not found in table '{table_name}'")
        
        # Calculate sum of numerical values (excluding first column which is the label)
        numerical_sum = 0
        for value in target_row.iloc[1:]:  # Skip first column (label)
            if pd.notna(value):
                # Try to extract numerical value
                numeric_value = self._extract_numeric_value(value)
                if numeric_value is not None:
                    numerical_sum += numeric_value
        
        return numerical_sum
    
    def _extract_numeric_value(self, value: Any) -> float:
        """Extract numeric value from various formats"""
        if isinstance(value, (int, float)):
            return float(value)
        
        if isinstance(value, str):
            # Remove common non-numeric characters
            cleaned = re.sub(r'[^\d.-]', '', value)
            if cleaned:
                try:
                    return float(cleaned)
                except ValueError:
                    pass
        
        return None

# Initialize Excel processor
processor = None

def initialize_processor():
    """Initialize the Excel processor"""
    global processor
    try:
        processor = ExcelProcessor(EXCEL_FILE_PATH)
        processor.load_excel_file()
        return True
    except Exception as e:
        print(f"Error initializing processor: {e}")
        return False

@app.on_event("startup")
async def startup_event():
    """Initialize the application on startup"""
    success = initialize_processor()
    if not success:
        print("Warning: Could not initialize Excel processor. Some endpoints may not work.")

@app.get("/")
async def root():
    """Root endpoint providing API information"""
    return {
        "message": "Excel Processing API",
        "version": "1.0.0",
        "endpoints": {
            "list_tables": "/list_tables",
            "get_table_details": "/get_table_details?table_name=<table_name>",
            "row_sum": "/row_sum?table_name=<table_name>&row_name=<row_name>"
        }
    }

@app.get("/list_tables")
async def list_tables():
    """List all table names present in the Excel sheet"""
    global processor
    
    if processor is None:
        raise HTTPException(status_code=500, detail="Excel processor not initialized")
    
    try:
        tables = processor.get_table_names()
        return {"tables": tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving tables: {str(e)}")

@app.get("/get_table_details")
async def get_table_details(table_name: str = Query(..., description="Name of the table")):
    """Get row names for a specific table"""
    global processor
    
    if processor is None:
        raise HTTPException(status_code=500, detail="Excel processor not initialized")
    
    try:
        row_names = processor.get_table_row_names(table_name)
        return {
            "table_name": table_name,
            "row_names": row_names
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving table details: {str(e)}")

@app.get("/row_sum")
async def row_sum(
    table_name: str = Query(..., description="Name of the table"),
    row_name: str = Query(..., description="Name of the row")
):
    """Calculate sum of numerical values in a specific row"""
    global processor
    
    if processor is None:
        raise HTTPException(status_code=500, detail="Excel processor not initialized")
    
    try:
        sum_value = processor.calculate_row_sum(table_name, row_name)
        return {
            "table_name": table_name,
            "row_name": row_name,
            "sum": sum_value
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error calculating row sum: {str(e)}")

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "processor_initialized": processor is not None}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=9090)