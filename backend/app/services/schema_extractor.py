import pandas as pd
import numpy as np
import io
from typing import Dict, List, Tuple, Optional
from sqlalchemy import Table, Column, Integer, String, Float, Date, Boolean, MetaData, ForeignKey
from datetime import datetime
import logging
import re
import math 

logger = logging.getLogger(__name__)

def clean_for_json(obj):
    """
    Clean values for JSON serialization by converting NaN/inf to None
    """
    if isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_for_json(item) for item in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    elif isinstance(obj, (np.integer, np.floating)):
        val = obj.item()
        if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
            return None
        return val
    elif pd.isna(obj):
        return None
    return obj

    
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
        sample_size: int = 1000,
        header_row: int = 0,
        skip_rows: Optional[List[int]] = None,
        override_primary_key: Optional[str] = None,
        override_foreign_keys: Optional[List[Dict]] = None
    ) -> Dict:
        """
        Extract schema from CSV file
        
        Args:
            header_row: Row number is the Original file (0indexed)
            skip_row: Row numbers in the Original file to skip
            override_primary_key: Column name to use as primary key (or None)
            override_foreign_keys: List of FK definitions like: [{"column": "patient_id", "ref_table": "patients", "ref_column": "id"}]
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
                
            table_name = self._sanitize_table_name(filename)
            columns = self._analyze_columns(df, sample_size)
            
            
            if override_primary_key is not None:
                if override_primary_key == "":
                    primary_key = ""
                    logger.info("User chose: No primary key")
                else:
                    column_names = [col['name'] for col in columns]
                    if override_primary_key not in [col['name'] for col in columns]:
                        return {
                            "success": False,
                            "error": f"Primary key column '{override_primary_key}' not found"
                        }
                    primary_key = override_primary_key
                    logger.info(f"User chose primary key: {primary_key}")
            else:
                primary_key = None
                logger.info("No override (None) - will add auto-increment 'id' column")

            if override_foreign_keys is not None:
                foreign_keys = override_foreign_keys
            else:
                foreign_keys = [] 
            
            create_sql = self._generate_create_table_sql(
                table_name, columns, primary_key, foreign_keys
            )

            logger.info(f"Generated SQL includes auto-increment: {'id SERIAL' in create_sql}")

            sample_df = df.head(5).replace([np.inf, -np.inf], np.nan) 
            sample_data = sample_df.to_dict('records')
            
            result = {
                "success": True,
                "table_name": table_name,
                "row_count": len(df),
                "columns": columns,
                "primary_key": primary_key,
                "foreign_keys": foreign_keys,
                "create_sql": create_sql,
                "sample_data": sample_data
            }

            return clean_for_json(result)
            
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
            clean_column = df[col_name].replace([np.inf, -np.inf], np.nan)
            
            sample = clean_column.head(sample_size)
            
            col_type, sql_type = self._detect_column_type(sample)
            
            has_nulls = bool(clean_column.isnull().any())
            null_count = int(clean_column.isnull().sum())
            
            is_unique = bool(clean_column.nunique() == len(df))
            
            max_length = None
            if sql_type == 'VARCHAR':
                max_length = int(clean_column.astype(str).str.len().max())
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
                "unique_count": int(clean_column.nunique()),
                "sample_values": sample_values
            })
        
        return columns

    
    def _detect_column_type(self, series: pd.Series) -> Tuple[str, str]:
        """Detect the most appropriate SQL type for a column"""
        pandas_type = str(series.dtype)
        
        if pandas_type == 'object':
            try:
                pd.to_datetime(series.dropna().head(100), format = 'mixed')
                return 'datetime64[ns]', 'DATE'
            except:
                pass
            
            try:
                numeric_series = pd.to_numeric(series.dropna().head(100))
                if (numeric_series % 1 == 0).all():
                    max_val = numeric_series.max()
                    if abs(max_val) > 2147483647:  
                        return 'int64', 'BIGINT'
                    else:
                        return 'int64', 'INTEGER'
                else:
                    return 'float64', 'FLOAT'
            except:
                pass
            
            return 'object', 'VARCHAR'
        
        if pandas_type == 'int64':
            max_val = series.max()
            if abs(max_val) > 2147483647:
                return 'int64', 'BIGINT'
            else:
                return 'int64', 'INTEGER'
        
        if pandas_type == 'float64':
            if (series.dropna() % 1 == 0).all():
                max_val = series.max()
                if abs(max_val) > 2147483647:
                    return 'int64', 'BIGINT'
            return 'float64', 'FLOAT'
        
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
        """
        Generate CREATE TABLE SQL statement
        
        If no primary_key provided, automatically adds 'id SERIAL PRIMARY KEY'
        """
        col_definitions = []
        
        if not primary_key:
            col_definitions.append("id SERIAL PRIMARY KEY")
            logger.info(f"No primary key specified for {table_name}, adding auto-increment 'id' column")
        
        for col in columns:
            col_name = col['name']
            sql_type = col['sql_type']
            
            if sql_type == 'VARCHAR' and col.get('max_length'):
                sql_type = f"VARCHAR({col['max_length']})"
            
            col_def = f"{col_name} {sql_type}"
            
            if not col['nullable']:
                col_def += " NOT NULL"
            
            if primary_key and col_name == primary_key:
                col_def += " PRIMARY KEY"
            
            col_definitions.append(col_def)
        
        for fk in foreign_keys:
            fk_def = f"FOREIGN KEY ({fk['column']}) REFERENCES {fk['ref_table']}({fk['ref_column']})"
            col_definitions.append(fk_def)
        
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n    "
        sql += ",\n    ".join(col_definitions)
        sql += "\n);"
        
        return sql


schema_extractor = SchemaExtractor()