import pandas as pd
import numpy as np
import io
from typing import Dict, List, Tuple, Optional
from sqlalchemy import Table, Column, Integer, String, Float, Date, Boolean, MetaData, ForeignKey
from datetime import datetime
import logging
import re

logger = logging.getLogger(__name__)


class SchemaExtractor:
    """Extract database schema from CSV files"""
    
    def __init__(self):
        self.type_mapping = {
            'int64': 'INTEGER',
            'float64': 'FLOAT',
            'object': 'VARCHAR',
            'bool': 'BOOLEAN',
            'datetime64[ns]': 'DATE',
        }
    
    @staticmethod
    def convert_numpy_types(obj):
        """Convert numpy types to native Python types for JSON serialization"""
        if isinstance(obj, (np.integer, np.int64, np.int32, np.int16, np.int8)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64, np.float32)):
            return float(obj)
        elif isinstance(obj, np.bool_):
            return bool(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, dict):
            return {key: SchemaExtractor.convert_numpy_types(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [SchemaExtractor.convert_numpy_types(item) for item in obj]
        elif pd.isna(obj):
            return None
        return obj
    
    def extract_from_csv(
        self,
        file_content: bytes,
        filename: str,
        sample_size: int = 1000
    ) -> Dict:
        """
        Extract schema from CSV file
        
        Args:
            file_content: CSV file content as bytes
            filename: Name of the file
            sample_size: Number of rows to analyze
        
        Returns:
            Dict with schema information
        """
        try:
           
            df = pd.read_csv(io.BytesIO(file_content))
            
            
            table_name = self._sanitize_table_name(filename)
            
           
            columns = self._analyze_columns(df, sample_size)
            
            
            primary_key = self._detect_primary_key(df, columns)
            
            
            foreign_keys = self._detect_foreign_keys(columns)
            
            
            create_sql = self._generate_create_table_sql(
                table_name, columns, primary_key, foreign_keys
            )
            
           
            sample_data = df.head(5).to_dict('records')
            sample_data = self.convert_numpy_types(sample_data)
            
            result = {
                "success": True,
                "table_name": table_name,
                "row_count": int(len(df)),
                "columns": columns,
                "primary_key": primary_key,
                "foreign_keys": foreign_keys,
                "create_sql": create_sql,
                "sample_data": sample_data
            }
            
           
            return self.convert_numpy_types(result)
            
        except Exception as e:
            logger.error(f"Schema extraction error: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def _sanitize_table_name(self, filename: str) -> str:
        """Convert filename to valid table name"""
    
        name = filename.rsplit('.', 1)[0]
        
        
        name = re.sub(r'[^\w]', '_', name)
        
    
        name = name.lower()
        
       
        if name[0].isdigit():
            name = 'table_' + name
        
        return name
    
    def _analyze_columns(self, df: pd.DataFrame, sample_size: int) -> List[Dict]:
        """Analyze each column to determine type and properties"""
        columns = []
        
        for col_name in df.columns:
            
            sample = df[col_name].head(sample_size)
            
        
            col_type, sql_type = self._detect_column_type(sample)
            
           
            has_nulls = bool(df[col_name].isnull().any())
            null_count = int(df[col_name].isnull().sum())
            
            
            is_unique = bool(df[col_name].nunique() == len(df))
            
            
            max_length = None
            if sql_type == 'VARCHAR':
                max_length = int(df[col_name].astype(str).str.len().max())
               
                max_length = self._round_varchar_length(max_length)
            
          
            sample_values = sample.dropna().head(3).tolist()
            sample_values = self.convert_numpy_types(sample_values)
            
            columns.append({
                "name": self._sanitize_column_name(col_name),
                "original_name": col_name,
                "pandas_type": str(col_type),
                "sql_type": sql_type,
                "max_length": max_length,
                "nullable": has_nulls,
                "null_count": null_count,
                "unique": is_unique,
                "unique_count": int(df[col_name].nunique()),
                "sample_values": sample_values
            })
        
        return columns
    
    def _detect_column_type(self, series: pd.Series) -> Tuple[str, str]:
        """Detect the most appropriate SQL type for a column"""
        pandas_type = str(series.dtype)
        
        
        if pandas_type == 'object':
            
            try:
                pd.to_datetime(series.dropna().head(100))
                return 'datetime64[ns]', 'DATE'
            except:
                pass
            
          
            try:
                numeric_series = pd.to_numeric(series.dropna().head(100))
                if (numeric_series % 1 == 0).all():
                    return 'int64', 'INTEGER'
                else:
                    return 'float64', 'FLOAT'
            except:
                pass
            
            
            return 'object', 'VARCHAR'
        
 
        sql_type = self.type_mapping.get(pandas_type, 'TEXT')
        
        return pandas_type, sql_type
    
    def _round_varchar_length(self, length: int) -> int:
        """Round VARCHAR length to sensible value"""
        if length <= 50:
            return 50
        elif length <= 100:
            return 100
        elif length <= 255:
            return 255
        elif length <= 500:
            return 500
        elif length <= 1000:
            return 1000
        else:
            return 2000
    
    def _sanitize_column_name(self, name: str) -> str:
        """Convert column name to valid SQL identifier"""
      
        name = re.sub(r'[^\w]', '_', name)
        
       
        name = name.lower()
        
        
        if name[0].isdigit():
            name = 'col_' + name
        
       
        sql_keywords = ['select', 'from', 'where', 'table', 'order', 'group']
        if name in sql_keywords:
            name = name + '_col'
        
        return name
    
    def _detect_primary_key(
        self,
        df: pd.DataFrame,
        columns: List[Dict]
    ) -> Optional[str]:
        """Detect potential primary key column"""
        
       
        for col in columns:
            col_name = col['name'].lower()
            if col_name == 'id' and col['unique']:
                return col['name']
        
       
        for col in columns:
            if (col['unique'] and 
                not col['nullable'] and 
                col['sql_type'] == 'INTEGER'):
                return col['name']
        
      
        for col in columns:
            if col['unique'] and not col['nullable']:
                return col['name']
        
        return None
    
    def _detect_foreign_keys(self, columns: List[Dict]) -> List[Dict]:
        """Detect potential foreign key relationships"""
        foreign_keys = []
        
        for col in columns:
            col_name = col['name'].lower()
            
            
            if col_name.endswith('_id') and col_name != 'id':
              
                ref_table = col_name[:-3] 
                
                foreign_keys.append({
                    "column": col['name'],
                    "references_table": ref_table,
                    "references_column": "id"
                })
        
        return foreign_keys
    
    def _generate_create_table_sql(
        self,
        table_name: str,
        columns: List[Dict],
        primary_key: Optional[str],
        foreign_keys: List[Dict]
    ) -> str:
        """Generate CREATE TABLE SQL statement"""
        
        sql_parts = [f"CREATE TABLE IF NOT EXISTS {table_name} ("]
        
        column_defs = []
        
        for col in columns:
            col_def = f"    {col['name']}"
            
            if col['sql_type'] == 'VARCHAR' and col['max_length']:
                col_def += f" VARCHAR({col['max_length']})"
            else:
                col_def += f" {col['sql_type']}"
            
            if not col['nullable']:
                col_def += " NOT NULL"
            
            column_defs.append(col_def)
        
        if primary_key:
            column_defs.append(f"    PRIMARY KEY ({primary_key})")
        
        for fk in foreign_keys:
            fk_def = f"    FOREIGN KEY ({fk['column']}) REFERENCES {fk['references_table']}({fk['references_column']})"
            column_defs.append(fk_def)
        
        sql_parts.append(",\n".join(column_defs))
        sql_parts.append(");")
        
        return "\n".join(sql_parts)


schema_extractor = SchemaExtractor()