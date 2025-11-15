import pandas as pd
import io
from sqlalchemy import create_engine, text, Table, MetaData
from sqlalchemy.exc import SQLAlchemyError
from typing import Dict
import logging

from app.config import settings
from app.models.database import engine

logger = logging.getLogger(__name__)


class DataLoader:
    """Load CSV data into PostgreSQL"""
    
    def __init__(self):
        self.engine = engine
    
    def create_table_from_sql(self, create_sql: str) -> Dict:
        """
        Execute CREATE TABLE SQL statement
        
        Args:
            create_sql: CREATE TABLE SQL statement
        
        Returns:
            Dict with result
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text(create_sql))
                conn.commit()
            
            return {
                "success": True,
                "message": "Table created successfully"
            }
        except SQLAlchemyError as e:
            logger.error(f"Error creating table: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def load_csv_data(
        self,
        file_content: bytes,
        table_name: str,
        if_exists: str = 'append'
    ) -> Dict:
        """
        Load CSV data into database table using pandas
        
        Args:
            file_content: CSV file content as bytes
            table_name: Name of the table
            if_exists: What to do if table exists ('fail', 'replace', 'append')
        
        Returns:
            Dict with result
        """
        try:
            df = pd.read_csv(io.BytesIO(file_content))
            
            df.columns = [self._sanitize_column_name(col) for col in df.columns]
            
            rows_inserted = df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists=if_exists,
                index=False,
                method='multi',  
                chunksize=1000
            )
            
            return {
                "success": True,
                "rows_inserted": len(df),
                "message": f"Successfully loaded {len(df)} rows into {table_name}"
            }
            
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def _sanitize_column_name(self, name: str) -> str:
        """Convert column name to valid SQL identifier"""
        import re
        name = re.sub(r'[^\w]', '_', name)
        name = name.lower()
        if name[0].isdigit():
            name = 'col_' + name
        return name


data_loader = DataLoader()
