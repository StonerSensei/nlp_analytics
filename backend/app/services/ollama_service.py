import ollama
from typing import Optional, Dict, Any
import logging
from app.config import settings

logger = logging.getLogger(__name__)


class OllamaService:
    """Service for interacting with Ollama LLM"""
    
    def __init__(self):
        self.client = ollama.Client(host=settings.OLLAMA_URL)
        self.model = settings.OLLAMA_MODEL
    
    def test_connection(self) -> bool:
        """Test if Ollama is accessible"""
        try:
            models = self.client.list()
            return True
        except Exception as e:
            logger.error(f"Ollama connection test failed: {e}")
            return False
    
    def list_models(self) -> list:
        """Get list of available models"""
        try:
            response = self.client.list()
            return response.get('models', [])
        except Exception as e:
            logger.error(f"Error listing models: {e}")
            return []
    
    def model_exists(self, model_name: str = None) -> bool:
        """Check if a specific model exists"""
        model = model_name or self.model
        try:
            models = self.list_models()
            model_names = [m['name'] for m in models]
            return model in model_names
        except Exception as e:
            logger.error(f"Error checking model existence: {e}")
            return False
    
    def generate(
        self,
        prompt: str,
        model: str = None,
        stream: bool = False,
        options: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Generate text using Ollama
        
        Args:
            prompt: The prompt to send to the model
            model: Model name (defaults to configured model)
            stream: Whether to stream the response
            options: Additional options for generation
        
        Returns:
            Dict containing the response
        """
        model = model or self.model
        
        try:
            response = self.client.generate(
                model=model,
                prompt=prompt,
                stream=stream,
                options=options or {}
            )
            
            return {
                "success": True,
                "model": model,
                "response": response['response'],
                "context": response.get('context'),
                "total_duration": response.get('total_duration'),
                "prompt_eval_count": response.get('prompt_eval_count'),
                "eval_count": response.get('eval_count')
            }
        except Exception as e:
            logger.error(f"Generation error: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def chat(
        self,
        messages: list,
        model: str = None,
        stream: bool = False
    ) -> Dict[str, Any]:
        """
        Chat with Ollama (maintains conversation context)
        
        Args:
            messages: List of message dicts with 'role' and 'content'
            model: Model name
            stream: Whether to stream
        
        Returns:
            Dict containing the response
        """
        model = model or self.model
        
        try:
            response = self.client.chat(
                model=model,
                messages=messages,
                stream=stream
            )
            
            return {
                "success": True,
                "model": model,
                "message": response['message'],
                "total_duration": response.get('total_duration')
            }
        except Exception as e:
            logger.error(f"Chat error: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def generate_sql(
        self,
        question: str,
        schema: str,
        context: str = ""
    ) -> Dict[str, Any]:
        """
        Generate SQL query from natural language question
        
        Args:
            question: Natural language question
            schema: Database schema information
            context: Additional context (optional)
        
        Returns:
            Dict with generated SQL query
        """
        prompt = self._build_sql_prompt(question, schema, context)
        
        response = self.generate(
            prompt=prompt,
            options={
                "temperature": 0.1,  
                "num_predict": 500,  
            }
        )
        
        if response['success']:
            sql = self._extract_sql(response['response'])
            return {
                "success": True,
                "sql": sql,
                "raw_response": response['response'],
                "model": response['model']
            }
        else:
            return response
    
    def _build_sql_prompt(
        self,
        question: str,
        schema: str,
        context: str = ""
    ) -> str:
        """Build prompt for SQL generation"""
        context_section = f"### Additional Context:\n{context}\n\n" if context else ""

        import re
        table_names = re.findall(r'CREATE TABLE "?(\w+)"?', schema)
        table_list = "\n".join([f'  - "{t}" (EXACT name, do not add "s")' for t in table_names])
        
        prompt = f"""### Instructions:
Convert the question into a valid PostgreSQL SELECT query.

CRITICAL - TABLE NAMES:
{table_list}
RULES:
1. Use EXACT table names listed above - NEVER add 's' to pluralize
2. Always use double quotes: "table_name"
3. Return ONLY the SQL query - no explanations or markdown
4. Use table aliases for joins: FROM "table1" t1

### Database Schema:
{schema}

### Question:
{question}

{context_section}### SQL Query:
SELECT"""
        
        return prompt
    
    def _extract_sql(self, response: str) -> str:
        """Extract SQL query from response"""
        code_block_sql = "```sql"
        code_block_generic = "```"
        
        if code_block_sql in response:
            response = response.split(code_block_sql)[1].split(code_block_generic)[0]
        elif code_block_generic in response:
            response = response.split(code_block_generic)[1].split(code_block_generic)[0]
        
        sql = response.strip()
        
        if not sql.upper().startswith('SELECT'):
            sql = 'SELECT ' + sql
        
        return sql


ollama_service = OllamaService()