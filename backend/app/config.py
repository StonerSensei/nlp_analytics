from pydantic_settings import BaseSettings
from typing import Optional
import os

class Settings(BaseSettings):
    POSTGRES_USER: str = "hospital_admin"
    POSTGRES_PASSWORD: str = "secure_pass_123"
    POSTGRES_DB: str = "hospital_db"
    POSTGRES_HOST: str = "postgres"
    POSTGRES_PORT: int = 5432
    
    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
    
    OLLAMA_URL: str = "http://ollama:11434"
    OLLAMA_MODEL: str = "sqlcoder:7b"
    
    METABASE_URL: str = "http://metabase:3000"
    
    ENVIRONMENT: str = "development"
    UPLOAD_DIR: str = "/app/uploads"
    MAX_UPLOAD_SIZE: int = 100 * 1024 * 1024  # 100 MB
    ALLOWED_EXTENSIONS: list = [".csv", ".xlsx", ".xls"]
    
    DB_POOL_SIZE: int = 5
    DB_MAX_OVERFLOW: int = 10
    DB_POOL_TIMEOUT: int = 30
    DB_POOL_RECYCLE: int = 3600
    
    class Config:
        env_file = ".env"
        case_sensitive = True

settings = Settings()
