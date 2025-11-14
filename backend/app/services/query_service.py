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
    
    def generate_database_schema_context(self) -> str:
        """
        Generate a comprehensive schema description for the LLM
        
        Returns:
            String containing database schema information
        """
        try:
            inspector = inspect(self.engine)
            tables = inspector.get_table_names()
            
            if not tables:
                return "No tables found in database."
            
            schema_parts = []
            
            for table_name in tables:
                # Get columns
                columns = inspector.get_columns(table_name)
                pk_constraint = inspector.get_pk_constraint(table_name)
                foreign_keys = inspector.get_foreign_keys(table_name)
                
                # Build CREATE TABLE statement
                col_defs = []
                for col in columns:
                    col_def = f"{col['name']} {col['type']}"
                    if not col.get('nullable', True):
                        col_def += " NOT NULL"
                    col_defs.append(col_def)
                
                # Add primary key
                if pk_constraint and pk_constraint.get('constrained_columns'):
                    pk_cols = ', '.join(pk_constraint['constrained_columns'])
                    col_defs.append(f"PRIMARY KEY ({pk_cols})")
                
                # Add foreign keys
                for fk in foreign_keys:
                    fk_cols = ', '.join(fk['constrained_columns'])
                    ref_table = fk['referred_table']
                    ref_cols = ', '.join(fk['referred_columns'])
                    col_defs.append(f"FOREIGN KEY ({fk_cols}) REFERENCES {ref_table}({ref_cols})")
                
                table_schema = f"CREATE TABLE {table_name} (\n    " + ",\n    ".join(col_defs) + "\n);"
                schema_parts.append(table_schema)
            
            return "\n\n".join(schema_parts)
            
        except Exception as e:
            logger.error(f"Error generating schema context: {e}")
            return "Error retrieving database schema."
    
    def query_from_natural_language(
        self,
        question: str,
        execute: bool = True,
        limit: Optional[int] = 100
    ) -> Dict:
        """
        Convert natural language question to SQL and optionally execute it
        
        Args:
            question: Natural language question
            execute: Whether to execute the generated SQL
            limit: Maximum number of rows to return
        
        Returns:
            Dict with SQL query and results
        """
        try:
            # Get database schema
            schema = self.generate_database_schema_context()
            
            if not schema or schema == "No tables found in database.":
                return {
                    "success": False,
                    "error": "No tables found in database. Please upload CSV files first.",
                    "suggestion": "Upload CSV files using POST /api/upload/"
                }
            
            # Generate SQL using Ollama
            sql_result = self.ollama.generate_sql(
                question=question,
                schema=schema,
                context=f"Limit results to {limit} rows if not specified in the question."
            )
            
            if not sql_result['success']:
                return {
                    "success": False,
                    "error": "Failed to generate SQL query",
                    "details": sql_result.get('error')
                }
            
            generated_sql = sql_result['sql']
            
            # Add LIMIT if not present and limit is specified
            if limit and 'LIMIT' not in generated_sql.upper():
                generated_sql = generated_sql.rstrip(';') + f" LIMIT {limit};"
            
            response = {
                "success": True,
                "question": question,
                "sql": generated_sql,
                "raw_llm_response": sql_result.get('raw_response', ''),
                "model": sql_result.get('model', '')
            }
            
            # Execute query if requested
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
        """
        Safely execute a generated SQL query
        
        Args:
            sql: SQL query to execute
        
        Returns:
            Dict with execution results
        """
        try:
            # Safety check: only allow SELECT queries
            if not sql.strip().upper().startswith('SELECT'):
                return {
                    "executed": False,
                    "error": "Only SELECT queries are allowed for safety"
                }
            
            # Execute query
            with self.engine.connect() as conn:
                result = conn.execute(text(sql))
                
                # Fetch results
                rows = result.fetchall()
                columns = list(result.keys())
                
                # Convert to list of dicts
                data = []
                for row in rows:
                    data.append(dict(zip(columns, row)))
                
                return {
                    "executed": True,
                    "row_count": len(data),
                    "columns": columns,
                    "data": data,
                    "execution_time_ms": None  # Can add timing if needed
                }
                
        except SQLAlchemyError as e:
            logger.error(f"Query execution error: {e}")
            return {
                "executed": False,
                "error": f"Query execution failed: {str(e)}",
                "suggestion": "The generated SQL might be invalid. Try rephrasing your question."
            }
    
    def suggest_questions(self) -> List[str]:
        """
        Generate suggested questions based on database schema
        
        Returns:
            List of suggested questions
        """
        try:
            tables = get_all_tables()
            
            if not tables:
                return ["Upload some CSV files first to get started!"]
            
            suggestions = [
                f"How many records are in the {tables[0]} table?",
                f"Show me the first 10 rows from {tables[0]}",
            ]
            
            # Add table-specific suggestions
            for table in tables[:3]:  # Limit to first 3 tables
                table_info = get_table_info(table)
                if table_info and 'columns' in table_info:
                    columns = [col['name'] for col in table_info['columns']]
                    
                    # Suggest counting
                    suggestions.append(f"Count all records in {table}")
                    
                    # Suggest grouping if there are string columns
                    string_cols = [col['name'] for col in table_info['columns'] 
                                 if 'VARCHAR' in str(col['type']) or 'TEXT' in str(col['type'])]
                    if string_cols:
                        suggestions.append(f"Group {table} by {string_cols[0]}")
                    
                    # Suggest ordering if there are numeric columns
                    numeric_cols = [col['name'] for col in table_info['columns']
                                  if 'INT' in str(col['type']) or 'FLOAT' in str(col['type'])]
                    if numeric_cols and len(numeric_cols) > 1:
                        suggestions.append(f"Show {table} ordered by {numeric_cols[0]} descending")
            
            return suggestions[:10]  # Return max 10 suggestions
            
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


# Global instance
query_service = QueryService()
