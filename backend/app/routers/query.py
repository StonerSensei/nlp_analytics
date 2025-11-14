from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Optional, List
import logging

from app.services.query_service import query_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/query", tags=["Query"])


class NaturalLanguageQuery(BaseModel):
    question: str = Field(..., description="Natural language question about your data")
    execute: bool = Field(True, description="Whether to execute the generated SQL")
    limit: Optional[int] = Field(100, description="Maximum number of rows to return")


@router.post("/")
async def query_database(query: NaturalLanguageQuery):
    """
    Query database using natural language
    
    Ask questions about your data in plain English, and get SQL queries + results back.
    
    **Example questions:**
    - "How many employees are there?"
    - "Show me all employees in the Engineering department"
    - "What is the average salary by department?"
    - "List the top 5 highest paid employees"
    """
    try:
        result = query_service.query_from_natural_language(
            question=query.question,
            execute=query.execute,
            limit=query.limit
        )
        
        if not result['success']:
            raise HTTPException(
                status_code=400 if 'No tables found' in result.get('error', '') else 500,
                detail=result.get('error', 'Query failed')
            )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Query endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/schema")
async def get_database_schema():
    """
    Get the current database schema
    
    Returns the schema information that will be used for SQL generation
    """
    try:
        schema = query_service.generate_database_schema_context()
        
        return {
            "success": True,
            "schema": schema,
            "tables": query_service.get_query_stats()
        }
        
    except Exception as e:
        logger.error(f"Error getting schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/suggestions")
async def get_question_suggestions():
    """
    Get suggested questions based on database schema
    
    Returns a list of example questions you can ask about your data
    """
    try:
        suggestions = query_service.suggest_questions()
        stats = query_service.get_query_stats()
        
        return {
            "success": True,
            "suggestions": suggestions,
            "database_stats": stats
        }
        
    except Exception as e:
        logger.error(f"Error getting suggestions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/explain")
async def explain_query(question: str = Query(..., description="Question to explain")):
    """
    Generate SQL without executing it
    
    See what SQL query would be generated for your question without running it
    """
    try:
        result = query_service.query_from_natural_language(
            question=question,
            execute=False
        )
        
        if not result['success']:
            raise HTTPException(status_code=400, detail=result.get('error'))
        
        return {
            "success": True,
            "question": question,
            "sql": result['sql'],
            "explanation": "This SQL query would be executed to answer your question"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Explain endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
