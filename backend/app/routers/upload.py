from fastapi import APIRouter, UploadFile, File, HTTPException, Form
from fastapi.responses import JSONResponse
from typing import Optional
import logging

from app.services.schema_extractor import schema_extractor
from app.services.data_loader import data_loader
from app.models.database import table_exists, get_table_info

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/upload", tags=["Upload"])


@router.post("/analyze")
async def analyze_csv(file: UploadFile = File(...)):
    """
    Analyze CSV file and extract schema without creating table
    
    Upload a CSV file to see its detected schema, data types, and primary/foreign keys.
    """
    # Validate file type
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed")
    
    try:
        # Read file content
        content = await file.read()
        
        # Extract schema
        result = schema_extractor.extract_from_csv(content, file.filename)
        
        if not result['success']:
            raise HTTPException(status_code=400, detail=result['error'])
        
        return result
        
    except Exception as e:
        logger.error(f"Error analyzing CSV: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/")
async def upload_and_create_table(
    file: UploadFile = File(...),
    table_name: Optional[str] = Form(None),
    if_exists: str = Form("fail")
):
    """
    Upload CSV file, create table, and load data
    
    Parameters:
    - **file**: CSV file to upload
    - **table_name**: Optional custom table name (default: derived from filename)
    - **if_exists**: What to do if table exists: 'fail', 'replace', or 'append'
    
    Process:
    1. Analyze CSV and extract schema
    2. Create table in database
    3. Load data into table
    """
    # Validate file type
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed")
    
    try:
        # Read file content
        content = await file.read()
        
        # Extract schema
        schema_result = schema_extractor.extract_from_csv(content, file.filename)
        
        if not schema_result['success']:
            raise HTTPException(status_code=400, detail=schema_result['error'])
        
        # Use custom table name if provided
        final_table_name = table_name or schema_result['table_name']
        
        # Check if table exists
        exists = table_exists(final_table_name)
        
        if exists and if_exists == "fail":
            raise HTTPException(
                status_code=409,
                detail=f"Table '{final_table_name}' already exists. Use if_exists='replace' or 'append' to override."
            )
        
        # Create or replace table
        if not exists or if_exists == "replace":
            # Update table name in SQL
            create_sql = schema_result['create_sql'].replace(
                schema_result['table_name'],
                final_table_name
            )
            
            # Drop table if replacing
            if exists and if_exists == "replace":
                with data_loader.engine.connect() as conn:
                    conn.execute(f"DROP TABLE IF EXISTS {final_table_name} CASCADE")
                    conn.commit()
            
            # Create table
            create_result = data_loader.create_table_from_sql(create_sql)
            
            if not create_result['success']:
                raise HTTPException(status_code=500, detail=create_result['error'])
        
        # Load data
        load_result = data_loader.load_csv_data(
            content,
            final_table_name,
            if_exists='append' if exists and if_exists == "append" else 'append'
        )
        
        if not load_result['success']:
            raise HTTPException(status_code=500, detail=load_result['error'])
        
        # Get final table info
        table_info = get_table_info(final_table_name)
        
        return {
            "success": True,
            "message": "CSV uploaded and processed successfully",
            "table_name": final_table_name,
            "rows_inserted": load_result['rows_inserted'],
            "schema": schema_result,
            "table_info": table_info
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading CSV: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/supported-formats")
async def get_supported_formats():
    """Get list of supported file formats"""
    return {
        "formats": [".csv"],
        "max_size_mb": 100,
        "note": "Excel support (.xlsx, .xls) can be added"
    }
