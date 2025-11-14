from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Dict, Any

from app.models.database import (
    get_db,
    get_database_info,
    test_connection,
    get_all_tables,
    get_table_info,
    get_table_row_count,
    execute_raw_query
)

router = APIRouter(prefix="/api/database", tags=["Database"])


@router.get("/info")
async def database_info():
    """Get database connection information"""
    return get_database_info()


@router.get("/test")
async def test_db_connection():
    """Test database connection"""
    is_connected = test_connection()
    if is_connected:
        return {"status": "success", "message": "Database connection is healthy"}
    else:
        raise HTTPException(status_code=503, detail="Database connection failed")


@router.get("/tables")
async def list_tables():
    """Get list of all tables in database"""
    tables = get_all_tables()
    return {
        "count": len(tables),
        "tables": tables
    }


@router.get("/tables/{table_name}")
async def get_table_details(table_name: str):
    """Get detailed information about a specific table"""
    info = get_table_info(table_name)
    if "error" in info:
        raise HTTPException(status_code=404, detail=info["error"])
    return info


@router.get("/tables/{table_name}/count")
async def get_table_count(table_name: str):
    """Get row count for a specific table"""
    count = get_table_row_count(table_name)
    return {
        "table": table_name,
        "row_count": count
    }


@router.get("/tables/{table_name}/sample")
async def get_table_sample(
    table_name: str,
    limit: int = 10,
    db: Session = Depends(get_db)
):
    """Get sample rows from a table"""
    try:
        query = text(f"SELECT * FROM {table_name} LIMIT :limit")
        result = db.execute(query, {"limit": limit})
        rows = [dict(row._mapping) for row in result]
        
        return {
            "table": table_name,
            "sample_size": len(rows),
            "data": rows
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/query")
async def execute_query(query: str, db: Session = Depends(get_db)):
    """
    Execute a raw SQL query (SELECT only)
    
    **WARNING:** Use with caution
    """
    try:
        result = execute_raw_query(query)
        return {
            "query": query,
            "row_count": len(result),
            "results": result
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query execution failed: {str(e)}")
