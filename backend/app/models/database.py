from sqlalchemy import create_engine, MetaData, inspect, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import NullPool
from typing import Generator
import logging

from app.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

engine = create_engine(
    settings.DATABASE_URL,
    pool_pre_ping=True,  
    pool_size=5,  
    max_overflow=10,  
    pool_timeout=30,  
    pool_recycle=3600,  
    echo=False,
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

metadata = MetaData()
     

def get_db() -> Generator[Session, None, None]:
    """
    Database session dependency for FastAPI
    
    Usage:
        @app.get("/endpoint")
        def endpoint(db: Session = Depends(get_db)):
            ...
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_database_info() -> dict:
    """Get database connection information"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version = result.scalar()
            
            return {
                "status": "connected",
                "database": settings.POSTGRES_DB,
                "host": settings.POSTGRES_HOST,
                "port": settings.POSTGRES_PORT,
                "version": version
            }
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return {
            "status": "error",
            "error": str(e)
        }


def test_connection() -> bool:
    """Test if database connection is working"""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        return False


def get_all_tables() -> list[str]:
    """Get list of all tables in database"""
    try:
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        return tables
    except Exception as e:
        logger.error(f"Error fetching tables: {e}")
        return []


def table_exists(table_name: str) -> bool:
    """Check if a table exists in database"""
    try:
        inspector = inspect(engine)
        return table_name in inspector.get_table_names()
    except Exception as e:
        logger.error(f"Error checking table existence: {e}")
        return False


def get_table_info(table_name: str) -> dict:
    """Get information about a specific table"""
    try:
        inspector = inspect(engine)
        
        if not table_exists(table_name):
            return {"error": f"Table '{table_name}' does not exist"}
        
        columns = inspector.get_columns(table_name)
        pk_constraint = inspector.get_pk_constraint(table_name)
        foreign_keys = inspector.get_foreign_keys(table_name)
        indexes = inspector.get_indexes(table_name)
        
        return {
            "table_name": table_name,
            "columns": columns,
            "primary_keys": pk_constraint.get('constrained_columns', []),
            "foreign_keys": foreign_keys,
            "indexes": indexes
        }
    except Exception as e:
        logger.error(f"Error getting table info: {e}")
        return {"error": str(e)}


def execute_raw_query(query: str, params: dict = None) -> list:
    """
    Execute raw SQL query (USE WITH CAUTION)
    Only for SELECT queries
    """
    if not query.strip().upper().startswith('SELECT'):
        raise ValueError("Only SELECT queries are allowed")
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            return [dict(row._mapping) for row in result]
    except Exception as e:
        logger.error(f"Query execution error: {e}")
        raise


def get_table_row_count(table_name: str) -> int:
    """Get row count for a table"""
    try:
        query = f"SELECT COUNT(*) FROM {table_name}"
        with engine.connect() as conn:
            result = conn.execute(text(query))
            return result.scalar()
    except Exception as e:
        logger.error(f"Error getting row count: {e}")
        return 0
