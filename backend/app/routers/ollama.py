from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict, Any

from app.services.ollama_service import ollama_service

router = APIRouter(prefix="/api/ollama", tags=["Ollama"])


class GenerateRequest(BaseModel):
    prompt: str
    model: Optional[str] = None
    temperature: Optional[float] = 0.7
    max_tokens: Optional[int] = 500


class ChatMessage(BaseModel):
    role: str  # 'user', 'assistant', or 'system'
    content: str


class ChatRequest(BaseModel):
    messages: List[ChatMessage]
    model: Optional[str] = None


class SQLGenerateRequest(BaseModel):
    question: str
    schema: str
    context: Optional[str] = ""


@router.get("/status")
async def ollama_status():
    """Check Ollama service status"""
    is_connected = ollama_service.test_connection()
    
    if not is_connected:
        raise HTTPException(status_code=503, detail="Ollama service is not available")
    
    return {
        "status": "connected",
        "url": ollama_service.client.host,
        "default_model": ollama_service.model
    }


@router.get("/models")
async def list_models():
    """List all available Ollama models"""
    models = ollama_service.list_models()
    
    return {
        "count": len(models),
        "models": models,
        "default_model": ollama_service.model
    }


@router.get("/models/{model_name}")
async def check_model(model_name: str):
    """Check if a specific model exists"""
    exists = ollama_service.model_exists(model_name)
    
    return {
        "model": model_name,
        "exists": exists
    }


@router.post("/generate")
async def generate_text(request: GenerateRequest):
    """
    Generate text using Ollama
    
    Example:
    ```
    {
        "prompt": "Explain what is SQL in one sentence",
        "temperature": 0.7,
        "max_tokens": 100
    }
    ```
    """
    response = ollama_service.generate(
        prompt=request.prompt,
        model=request.model,
        options={
            "temperature": request.temperature,
            "num_predict": request.max_tokens
        }
    )
    
    if not response['success']:
        raise HTTPException(status_code=500, detail=response['error'])
    
    return response


@router.post("/chat")
async def chat(request: ChatRequest):
    """
    Chat with Ollama (maintains conversation context)
    
    Example:
    ```
    {
        "messages": [
            {"role": "user", "content": "Hello, who are you?"},
            {"role": "assistant", "content": "I am an AI assistant."},
            {"role": "user", "content": "What can you do?"}
        ]
    }
    ```
    """
    messages = [msg.dict() for msg in request.messages]
    
    response = ollama_service.chat(
        messages=messages,
        model=request.model
    )
    
    if not response['success']:
        raise HTTPException(status_code=500, detail=response['error'])
    
    return response


@router.post("/generate-sql")
async def generate_sql(request: SQLGenerateRequest):
    """
    Generate SQL query from natural language question
    
    Example:
    ```
    {
        "question": "How many employees are there?",
        "schema": "CREATE TABLE employees (id INT, name VARCHAR, department VARCHAR);",
        "context": "Focus on active employees only"
    }
    ```
    """
    response = ollama_service.generate_sql(
        question=request.question,
        schema=request.schema,
        context=request.context
    )
    
    if not response['success']:
        raise HTTPException(status_code=500, detail=response['error'])
    
    return response


@router.post("/test")
async def test_ollama():
    """
    Quick test of Ollama service
    """
    if not ollama_service.test_connection():
        raise HTTPException(status_code=503, detail="Cannot connect to Ollama")
    
    model_exists = ollama_service.model_exists()
    
    if not model_exists:
        return {
            "status": "warning",
            "message": f"Ollama is connected but model '{ollama_service.model}' is not found",
            "suggestion": f"Run: docker exec -it hospital-ollama ollama pull {ollama_service.model}"
        }
    
    response = ollama_service.generate(
        prompt="Say 'Hello from Ollama!' and nothing else.",
        options={"num_predict": 20}
    )
    
    if response['success']:
        return {
            "status": "success",
            "message": "Ollama is working correctly",
            "test_response": response['response'],
            "model": response['model']
        }
    else:
        raise HTTPException(status_code=500, detail=response['error'])
