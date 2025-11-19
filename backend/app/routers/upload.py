from fastapi import APIRouter, UploadFile, File, HTTPException, Form
from fastapi.responses import JSONResponse
from typing import Optional
import logging
from app.services.csv_analyzer import csv_analyzer
from app.services.schema_extractor import schema_extractor
from app.services.data_loader import data_loader
from app.models.database import table_exists, get_table_info
import numpy as np

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/upload", tags=["Upload"])


@router.post("/analyze")
async def analyze_csv(file: UploadFile = File(...), header_row: int = Form(None),skip_rows: Optional[str] = Form(None)):
    """
    Analyze CSV schema with optional user-specified header row
    If header_row not provided, auto-detect
    
    Upload a CSV file to see its detected schema, data types, and primary/foreign keys.
    """
    try:
        content = await file.read()
        
        
        if header_row is None:
            analysis = csv_analyzer.analyze_file(content, preview_lines=20)
            if analysis['success']:
                header_row = analysis['detected_header_row']
                logger.info(f"Auto-detected header row: {header_row} (confidence: {analysis['confidence']}%)")
        
        skip_list = None
        if skip_rows:
            skip_list = [int(x.strip()) for x in skip_rows.split(',')]
        elif header_row and header_row > 0:
            skip_list = list(range(header_row))
        
        result = schema_extractor.extract_from_csv(
            file_content=content,
            filename=file.filename,
            header_row=header_row if header_row else 0,
            skip_rows=skip_list
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Analysis error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/")
async def upload_and_create_table(
    file: UploadFile = File(...),
    table_name: Optional[str] = Form(None),
    if_exists: str = Form("fail"),
    header_row: int = Form(0),                    
    skip_rows: Optional[str] = Form(None),
    primary_key: Optional[str] = Form(None),
    foreign_keys: Optional[str] = Form(None)         
):
    """
    Upload CSV file, create table, and load data
    
    Parameters:
    - **file**: CSV file to upload
    - **table_name**: Optional custom table name (default: derived from filename)
    - **if_exists**: What to do if table exists: 'fail', 'replace', or 'append'
    - **header_row**: Row number containing column names (0-indexed)
    - **skip_rows**: Comma-separated row numbers to skip
    - primary_key: Column name to set up as PK or empty string for no PK
    
    Process:
    1. Analyze CSV and extract schema
    2. Create table in database
    3. Load data into table
    """

    logger.info(f"Upload request - primary_key received: '{primary_key}' (type: {type(primary_key)})")

    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed")
    
    try:
        content = await file.read()
        
        skip_list = None
        if skip_rows:
            skip_list = [int(x.strip()) for x in skip_rows.split(',')]
        elif header_row and header_row > 0:
            skip_list = list(range(header_row))

        
        import json
        fk_list = None
        if foreign_keys:
            try:
                fk_list = json.loads(foreign_keys)
            except json.JSONDecodeError:
                raise HTTPException(status_code=400, detail="Invalid foreign_keys JSON format")
        
        override_pk = None
        if primary_key:
            if primary_key.upper() in ["__AUTO__", "__NONE__", "NONE", ""]:
                if "__NONE__" in primary_key.upper() or primary_key.upper() == "NONE":
                    override_pk = "" 
                    logger.info("Marker: No primary key")
                else:
                    override_pk = None 
                    logger.info("Marker: Auto-increment ID")
            else:
                override_pk = primary_key
                logger.info(f"Using column: {override_pk}")
        else:
            override_pk = None
            logger.info("Empty - auto-increment")

        logger.info(f"Calling schema_extractor with override_pk: {override_pk}")

        
        schema_result = schema_extractor.extract_from_csv(
            file_content=content,
            filename=file.filename,
            header_row=header_row,      
            skip_rows=skip_list,
            override_primary_key=override_pk,
            override_foreign_keys=fk_list          
        )
        
        if not schema_result['success']:
            raise HTTPException(status_code=400, detail=schema_result['error'])
        
        final_table_name = table_name or schema_result['table_name']
        
        exists = table_exists(final_table_name)
        
        if exists and if_exists == "fail":
            raise HTTPException(
                status_code=409,
                detail=f"Table '{final_table_name}' already exists. Use if_exists='replace' or 'append' to override."
            )
        
        if not exists or if_exists == "replace":
            create_sql = schema_result['create_sql'].replace(
                schema_result['table_name'],
                final_table_name
            )
            
            if exists and if_exists == "replace":
                from sqlalchemy import text
                with data_loader.engine.connect() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {final_table_name} CASCADE"))
                    conn.commit()
            
            create_result = data_loader.create_table_from_sql(create_sql)
            
            if not create_result['success']:
                raise HTTPException(status_code=500, detail=create_result['error'])
        
        load_result = data_loader.load_csv_data(
            content,
            final_table_name,
            if_exists='append' if exists and if_exists == "append" else 'append',
            header_row=header_row,      
            skip_rows=skip_list          
        )
        
        if not load_result['success']:
            raise HTTPException(status_code=500, detail=load_result['error'])
        
        table_info = get_table_info(final_table_name)
        
        return {
            "success": True,
            "message": "CSV uploaded and processed successfully",
            "table_name": final_table_name,
            "rows_inserted": load_result['rows_inserted']                                                                                                   
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

@router.post("/preview")
async def preview_csv(
    file: UploadFile = File(...),
    preview_lines: int = Form(20)
):
    """
    Analyze CSV and detect structure without uploading
    
    Returns:
        - Raw preview
        - Detected header row
        - Confidence score
        - Suggestions
    """
    try:
        content = await file.read()
        
        analysis = csv_analyzer.analyze_file(content, preview_lines)
        
        if not analysis['success']:
            raise HTTPException(status_code=400, detail=analysis.get('error'))
        
        return {
            "success": True,
            "filename": file.filename,
            "analysis": analysis
        }
        
    except Exception as e:
        logger.error(f"Preview error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/column-stats")
async def get_column_statistics(
    file: UploadFile = File(...),
    header_row: int = Form(0),
    skip_rows: Optional[str] = Form(None)
):
    """
    Get statistics for each column to help user choose primary key
    
    Returns uniqueness, null counts, data types for each column
    """
    try:
        content = await file.read()
        
        skip_list = None
        if skip_rows:
            skip_list = [int(x.strip()) for x in skip_rows.split(',')]
        elif header_row > 0:
            skip_list = list(range(header_row))
        
        
        adjusted_header = header_row
        if skip_list and header_row > 0:
            rows_before_header = sum(1 for skip in skip_list if skip < header_row)
            adjusted_header = header_row - rows_before_header
        
        import pandas as pd
        import io
        df = pd.read_csv(
            io.BytesIO(content),
            header=adjusted_header,
            skiprows=skip_list if skip_list else None
        )
        
        df = df.dropna(how='all')
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        df.columns = df.columns.str.strip()
        
        
        column_stats = []
        for col in df.columns:

            clean_series = df[col].replace([np.inf, -np.inf], np.nan)

            total_rows = len(clean_series)
            unique_count = int(clean_series.nunique())
            null_count = int(clean_series.isnull().sum())
            non_null_count = total_rows - null_count
            
            is_unique = unique_count == non_null_count
            has_nulls = null_count > 0
            suitable_for_pk = is_unique and not has_nulls
            
            column_stats.append({
                "name": col,
                "total_rows": int(total_rows),  
                "unique_count": int(unique_count),  
                "null_count": int(null_count),  
                "non_null_count": int(non_null_count),  
                "is_unique": bool(is_unique),  
                "has_nulls": bool(has_nulls),  
                "suitable_for_pk": bool(suitable_for_pk),  
                "uniqueness_percent": float(round((unique_count / total_rows) * 100, 1)) if total_rows > 0 else 0.0
            })
        
        return {
            "success": True,
            "column_stats": column_stats,
            "total_rows": int(len(df))
        }
        
    except Exception as e:
        logger.error(f"Column stats error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/validate-foreign-key")
async def validate_foreign_key(
    ref_table: str = Form(...),
    ref_column: str = Form(...)
):
    """
    Validate that a foreign key reference exists
    
    Checks if referenced table and column exist in database
    """
    try:
        from app.models.database import table_exists, get_table_info
        
        if not table_exists(ref_table):
            return {
                "valid": False,
                "error": f"Table '{ref_table}' does not exist",
                "suggestion": "Upload the referenced table first or check the spelling"
            }
        
        table_info = get_table_info(ref_table)
        columns = [col['name'] for col in table_info.get('columns', [])]
        
        if ref_column not in columns:
            return {
                "valid": False,
                "error": f"Column '{ref_column}' not found in table '{ref_table}'",
                "available_columns": columns
            }
        
        return {
            "valid": True,
            "message": f"Valid reference: {ref_table}.{ref_column}"
        }
        
    except Exception as e:
        logger.error(f"FK validation error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

