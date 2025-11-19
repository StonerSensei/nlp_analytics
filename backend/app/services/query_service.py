from typing import Dict, List, Optional
from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError
import logging

from app.models.database import engine, execute_raw_query, get_all_tables, get_table_info
from app.services.ollama_service import ollama_service

logger = logging.getLogger(__name__)


class QueryService:
    """Service for natural language to SQL query execution"""
    
    def __init__(self):
        self.engine = engine
        self.ollama = ollama_service
    
    def generate_database_schema_context(self, table_name: Optional[str] = None) -> str:
        """Generate a comprehensive schema description for the LLM"""
        try:
            inspector = inspect(self.engine)
            tables = inspector.get_table_names()
            
            if table_name:
                if table_name in tables:
                    tables = [table_name]
                else:
                    return f"Table '{table_name}' not found in database."
            
            if not tables:
                return "No tables found in database."
            
            schema_parts = []
            
            schema_parts.append("IMPORTANT: Use these EXACT table names (do not pluralize or modify):")
            for table in tables:
                schema_parts.append(f'  - "{table}"')
            schema_parts.append("")  
            
            for table in tables:
                columns = inspector.get_columns(table)
                pk_constraint = inspector.get_pk_constraint(table)
                foreign_keys = inspector.get_foreign_keys(table)
                
                col_defs = []
                for col in columns:
                    col_def = f"{col['name']} {col['type']}"
                    if not col.get('nullable', True):
                        col_def += " NOT NULL"
                    col_defs.append(col_def)
                
                if pk_constraint and pk_constraint.get('constrained_columns'):
                    pk_cols = ', '.join(pk_constraint['constrained_columns'])
                    col_defs.append(f"PRIMARY KEY ({pk_cols})")
                
                for fk in foreign_keys:
                    fk_cols = ', '.join(fk['constrained_columns'])
                    ref_table = fk['referred_table']
                    ref_cols = ', '.join(fk['referred_columns'])
                    col_defs.append(f"FOREIGN KEY ({fk_cols}) REFERENCES {ref_table}({ref_cols})")
                
                table_schema = f'CREATE TABLE "{table}" (\n    ' + ",\n    ".join(col_defs) + "\n);"
                schema_parts.append(table_schema)
            
            return "\n\n".join(schema_parts)
            
        except Exception as e:
            logger.error(f"Error generating schema context: {e}")
            return "Error retrieving database schema."


    
    def query_from_natural_language(
        self,
        question: str,
        execute: bool = True,
        limit: Optional[int] = 100,
        table_name: Optional[str] = None
    ) -> Dict:
        """
        Convert natural language question to SQL and optionally execute it
        
        Args:
            question: Natural language question
            execute: Whether to execute the generated SQL
            limit: Maximum number of rows to return
            table_name: Specific table to query (optional, filters schema)
        
        Returns:
            Dict with SQL query and results
        """
        try:
            schema = self.generate_database_schema_context(table_name=table_name)
            
            if not schema or schema == "No tables found in database.":
                return {
                    "success": False,
                    "error": "No tables found in database. Please upload CSV files first.",
                    "suggestion": "Upload CSV files using POST /api/upload/"
                }
            
            context_parts = [f"Limit results to {limit} rows if not specified in the question."]
        
            if table_name:
                context_parts.append(f"Focus on the '{table_name}' table.")
                context_parts.append(f"Use '{table_name}' as the primary table in your query.")
            
            context = " ".join(context_parts)
            
            sql_result = self.ollama.generate_sql(
                question=question,
                schema=schema,
                context=context
            )
            
            if not sql_result['success']:
                return {
                    "success": False,
                    "error": "Failed to generate SQL query",
                    "details": sql_result.get('error')
                }
            
            generated_sql = sql_result['sql']
            
            if limit and 'LIMIT' not in generated_sql.upper():
                generated_sql = generated_sql.rstrip(';') + f" LIMIT {limit};"
            
            response = {
                "success": True,
                "question": question,
                "sql": generated_sql,
                "raw_llm_response": sql_result.get('raw_response', ''),
                "model": sql_result.get('model', ''),
                "table_context": table_name if table_name else "all tables"
            }
            
            if execute:
                execution_result = self._execute_generated_query(generated_sql)
                response.update(execution_result)
            
            return response
            
        except Exception as e:
            logger.error(f"Error in natural language query: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def _execute_generated_query(self, sql: str) -> Dict:
        """Safely execute a generated SQL query"""
        try:
            if not sql.strip().upper().startswith('SELECT'):
                return {
                    "executed": False,
                    "error": "Only SELECT queries are allowed for safety"
                }
            
            sql = self._fix_table_names_in_sql(sql)
            
            with self.engine.connect() as conn:
                result = conn.execute(text(sql))
                
                rows = result.fetchall()
                columns = list(result.keys())
                
                data = []
                for row in rows:
                    data.append(dict(zip(columns, row)))
                
                return {
                    "executed": True,
                    "row_count": len(data),
                    "columns": columns,
                    "data": data,
                    "execution_time_ms": None
                }
                
        except SQLAlchemyError as e:
            logger.error(f"Query execution error: {e}")
            return {
                "executed": False,
                "error": f"Query execution failed: {str(e)}",
                "suggestion": "The generated SQL might be invalid. Try rephrasing your question."
            }

    def _fix_table_names_in_sql(self, sql: str) -> str:
        """Fix common table name mistakes in generated SQL"""
        try:
            import re
            inspector = inspect(self.engine)
            actual_tables = inspector.get_table_names()
            
            logger.info(f"Original SQL: {sql}")
            
            for actual_table in actual_tables:
                wrong_plural = actual_table + 's'
                
                replacements = [
                    (f'"{wrong_plural}"', f'"{actual_table}"'),      
                    (f' {wrong_plural} ', f' "{actual_table}" '),     
                    (f' {wrong_plural};', f' "{actual_table}";'),     
                    (f'FROM {wrong_plural}', f'FROM "{actual_table}"'),
                    (f'JOIN {wrong_plural}', f'JOIN "{actual_table}"'),
                ]
                
                for old, new in replacements:
                    if old in sql:
                        sql = sql.replace(old, new)
                        logger.warning(f"Fixed: {old} → {new}")
            
            logger.info(f"Fixed SQL: {sql}")
            return sql
            
        except Exception as e:
            logger.error(f"Error fixing table names: {e}")
            return sql





    
    def suggest_questions(self) -> List[str]:
        """Generate suggested questions based on database schema"""
        try:
            tables = get_all_tables()
            
            if not tables:
                return ["Upload some CSV files first to get started!"]
            
            suggestions = []
            
            for table in tables[:3]:
                
                suggestions.append(f"Show first 10 rows from {table}")
                suggestions.append(f"How many records are in {table}?")
                
                
                try:
                    table_info = get_table_info(table)
                    if table_info and table_info.get('columns'):
                        columns = table_info['columns']
                        
                        string_cols = [col['name'] for col in columns 
                                    if 'VARCHAR' in str(col.get('type', '')) or 'TEXT' in str(col.get('type', ''))]
                        if string_cols:
                            suggestions.append(f"Group {table} by {string_cols[0]}")
                        
                        numeric_cols = [col['name'] for col in columns
                                    if 'INT' in str(col.get('type', '')) or 'FLOAT' in str(col.get('type', ''))]
                        if numeric_cols:
                            suggestions.append(f"Show average {numeric_cols[0]} from {table}")
                except:
                    pass 
            
            return suggestions[:10]
            
        except Exception as e:
            logger.error(f"Error generating suggestions: {e}")
            return ["What data do you have?"]

    
    def get_query_stats(self) -> Dict:
        """Get statistics about the database for context"""
        try:
            tables = get_all_tables()
            
            stats = {
                "table_count": len(tables),
                "tables": []
            }
            
            for table in tables:
                with self.engine.connect() as conn:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    count = result.scalar()
                    
                    stats["tables"].append({
                        "name": table,
                        "row_count": count
                    })
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting query stats: {e}")
            return {"error": str(e)}


query_service = QueryService()
