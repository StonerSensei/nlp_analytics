from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    # PostgreSQL Configuration
    POSTGRES_USER: str = "hospital_admin"
    POSTGRES_PASSWORD: str = "secure_pass_123"
    POSTGRES_DB: str = "hospital_db"
    POSTGRES_HOST: str = "postgres"
    POSTGRES_PORT: int = 5432
    
    # Database URL (can be overridden or auto-constructed)
    DATABASE_URL: Optional[str] = None
    
    # Ollama Configuration
    OLLAMA_URL: str = "http://ollama:11434"
    OLLAMA_MODEL: str = "sqlcoder:7b"
    
    # Backend Configuration
    BACKEND_HOST: str = "0.0.0.0"
    BACKEND_PORT: int = 8000
    
    # Frontend Configuration
    FRONTEND_PORT: int = 8501
    BACKEND_API_URL: str = "http://backend:8000"
    
    # Metabase Configuration
    METABASE_URL: str = "http://metabase:3000"
    
    # Environment
    ENVIRONMENT: str = "development"
    
    @property
    def database_url(self):
        """Construct database URL from components"""
        if self.DATABASE_URL:
            return self.DATABASE_URL
        return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
    
    class Config:
        env_file = ".env"
        case_sensitive = False

settings = Settings()
