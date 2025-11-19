import pandas as pd
import io
from sqlalchemy import create_engine, text, Table, MetaData
from sqlalchemy.exc import SQLAlchemyError
from typing import Dict, Optional, List
import logging

from app.config import settings
from app.models.database import engine
import numpy as np
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
        if_exists: str = 'append',
        header_row: int = 0,
        skip_rows: Optional[List[int]] = None
    ) -> Dict:
        """
        Load CSV data into existing table
        
        Handles tables with or without auto-increment 'id' column
        """
        try:
            adjusted_header = header_row
            if skip_rows and header_row > 0:
                rows_before_header = sum(1 for skip in skip_rows if skip < header_row)
                adjusted_header = header_row - rows_before_header
            
            df = pd.read_csv(
                io.BytesIO(file_content),
                header=adjusted_header,
                skiprows=skip_rows if skip_rows else None
            )
            
            df = df.dropna(how='all')
            df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
            df.columns = df.columns.str.strip()

            df = df.replace([np.inf, -np.inf], np.nan)
        
            
            from sqlalchemy import inspect, Table, MetaData
            inspector = inspect(self.engine)
            table_columns = [col['name'] for col in inspector.get_columns(table_name)]

            has_auto_id = 'id' in table_columns and 'id' not in df.columns
        
            if has_auto_id:
                logger.info(f"Table {table_name} has auto-increment 'id' column")

            df.columns = [self._sanitize_column_name(col) for col in df.columns]
            columns_to_insert = [col for col in df.columns if col in table_columns]

            if not columns_to_insert:
                return {
                    "success": False,
                    "error": f"No matching columns. CSV: {list(df.columns)}, Table: {table_columns}"
                }
            
            logger.info(f"Inserting {len(columns_to_insert)} columns: {columns_to_insert}")

            df_to_insert = df[columns_to_insert]

            df_to_insert.to_sql(
                table_name,
                self.engine,
                if_exists='append',
                index=False,
                method='multi'
            )

            rows_inserted = len(df_to_insert)

            logger.info(f"Successfully inserted {rows_inserted} rows into {table_name}")

            return {
                "success": True,
                "rows_inserted": rows_inserted,
                "message": f"Loaded {rows_inserted} rows into {table_name}"
            }
        
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            return {
                "success": False,
                "error": str(e)
            }
            
            # # If table has 'id' column but CSV doesn't, that's the auto-increment column
            # # We should NOT include it in the insert (PostgreSQL auto-generates it)
            # if 'id' in table_columns and 'id' not in df.columns:
            #     logger.info(f"Table {table_name} has auto-increment 'id' column, will be auto-generated")
            
            # # Load table metadata
            # metadata = MetaData()
            # table = Table(table_name, metadata, autoload_with=self.engine)
            
            # # Convert dataframe to list of dicts
            # records = df.to_dict('records')
            
            # # Insert data
            # with self.engine.connect() as conn:
            #     result = conn.execute(table.insert(), records)
            #     conn.commit()
            #     rows_inserted = result.rowcount
            
            # logger.info(f"Successfully inserted {rows_inserted} rows into {table_name}")
            
            # return {
            #     "success": True,
            #     "rows_inserted": rows_inserted,
            #     "message": f"Loaded {rows_inserted} rows into {table_name}"
            # }
            
        # except Exception as e:
        #     logger.error(f"Error loading data: {e}")
        #     return {
        #         "success": False,
        #         "error": str(e)
        #     }

    
    def _sanitize_column_name(self, name: str) -> str:
        """Convert column name to valid SQL identifier"""
        import re
        name = re.sub(r'[^\w]', '_', name)
        name = name.lower()
        if name[0].isdigit():
            name = 'col_' + name
        return name


data_loader = DataLoader()
