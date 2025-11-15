from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
import uvicorn
from app.config import settings 

from app.models.database import get_db, test_connection, get_database_info
from app.services.ollama_service import ollama_service
from app.routers import database, ollama, upload, query

app = FastAPI(
    title="Hospital Analytics API",
    description="Natural Language to SQL API with Ollama",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(database.router)
app.include_router(ollama.router)
app.include_router(upload.router)
app.include_router(query.router)


@app.on_event("startup")
async def startup_event():
    """Run on application startup"""
    print("Starting Hospital Analytics API...")
    
    if test_connection():
        print("Database connection successful")
        db_info = get_database_info()
        print(f"Database: {db_info.get('database')}")
        print(f"Host: {db_info.get('host')}:{db_info.get('port')}")
    else:
        print("Database connection failed")
    
    if ollama_service.test_connection():
        print("Ollama connection successful")
        print(f"URL: {settings.OLLAMA_URL}")
        print(f"Default model: {ollama_service.model}")
        
        if ollama_service.model_exists():
            print(f"Model '{ollama_service.model}' is available")
        else:
            print(f"Model '{ollama_service.model}' not found")
            print(f"   Run: docker exec -it hospital-ollama ollama pull {ollama_service.model}")
    else:
        print("Ollama connection failed")


@app.on_event("shutdown")
async def shutdown_event():
    """Run on application shutdown"""
    print("Shutting down Hospital Analytics API...")


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    db_connected = test_connection()
    ollama_connected = ollama_service.test_connection()
    
    return {
        "status": "healthy" if (db_connected and ollama_connected) else "degraded",
        "service": "Hospital Analytics API",
        "version": "1.0.0",
        "database": "connected" if db_connected else "disconnected",
        "ollama": "connected" if ollama_connected else "disconnected"
    }


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Welcome to Hospital Analytics API",
        "docs": "/docs",
        "health": "/health",
        "database_info": "/api/database/info",
        "ollama_status": "/api/ollama/status"
    }


@app.get("/api/test-db")
async def test_database(db: Session = Depends(get_db)):
    """Test database session dependency"""
    try:
        result = db.execute("SELECT 'Hello from PostgreSQL!' as message")
        message = result.scalar()
        
        return {
            "status": "success",
            "message": message,
            "database": "PostgreSQL"
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
