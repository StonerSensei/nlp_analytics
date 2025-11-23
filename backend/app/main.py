from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, inspect
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import requests
import os
from typing import Optional
from app.config import settings

app = FastAPI(title="Hospital NLP-to-SQL API")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection
engine = create_engine(settings.DATABASE_URL)

@app.get("/health")
def health_check():
    return {"status": "healthy", "environment": settings.ENVIRONMENT}

@app.post("/upload-csv")
async def upload_csv(
    file: UploadFile = File(...),
    table_name: str = Form(...),
    skip_rows: int = Form(0)
):
    """Upload CSV and convert to PostgreSQL table"""
    try:
        # Save file temporarily
        upload_dir = "/app/uploads"
        os.makedirs(upload_dir, exist_ok=True)
        file_path = os.path.join(upload_dir, file.filename)
        
        with open(file_path, "wb") as f:
            content = await file.read()
            f.write(content)
        
        # Read CSV with skip rows
        df = pd.read_csv(file_path, skiprows=skip_rows)
        
        # Clean column names
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('/', '_').str.replace('.', '_')
        
        # Convert all columns to text type
        for col in df.columns:
            df[col] = df[col].astype(str).replace('nan', None)
        
        # Add auto-generated primary key
        df.insert(0, 'id', range(1, len(df) + 1))
        
        # Drop table if exists and create new one
        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
            conn.commit()
        
        # Write to database
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        
        # Add primary key constraint
        with engine.connect() as conn:
            conn.execute(text(f"ALTER TABLE {table_name} ADD PRIMARY KEY (id)"))
            conn.commit()
        
        return {
            "message": f"Table {table_name} created successfully",
            "rows": len(df),
            "columns": list(df.columns),
            "table_name": table_name
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tables")
def get_tables():
    """Get list of all tables"""
    try:
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        
        table_info = []
        for table in tables:
            columns = inspector.get_columns(table)
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                row_count = result.scalar()
            
            table_info.append({
                "name": table,
                "columns": [col['name'] for col in columns],
                "row_count": row_count
            })
        
        return {"tables": table_info}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/query")
async def natural_language_query(query: str = Form(...)):
    """Convert natural language to SQL and execute"""
    try:
        # Get database schema
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        
        schema_info = ""
        for table in tables:
            columns = inspector.get_columns(table)
            col_names = [col['name'] for col in columns]
            schema_info += f"Table: {table}\nColumns: {', '.join(col_names)}\n\n"
        
        # Create prompt for Ollama
        prompt = f"""You are a PostgreSQL expert. Convert the following natural language query to SQL.

Database Schema:
{schema_info}

Natural Language Query: {query}

Important Notes:
- In HIS table: 'bill_id' is the billing identifier
- In RIS table: 'patient_id' is the patient identifier  
- bill_id in HIS corresponds to patient_id in RIS (same values, different column names)
- Return ONLY the SQL query, no explanations
- Use proper table and column names from the schema above
- For finding discrepancies between HIS and RIS, compare unique bill_id counts with unique patient_id counts
- All data is stored as text, use appropriate string comparisons

SQL Query:"""

        # Call Ollama API
        ollama_response = requests.post(
            f"{settings.OLLAMA_HOST}/api/generate",
            json={
                "model": settings.OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.1
                }
            },
            timeout=90
        )
        
        if ollama_response.status_code != 200:
            raise HTTPException(status_code=500, detail="Failed to get response from Ollama")
        
        sql_query = sql_query.replace("``````", "").strip()
        # Remove any trailing semicolons or extra whitespace
        sql_query = sql_query.rstrip(';').strip()
        
        # Execute SQL query
        with engine.connect() as conn:
            result = conn.execute(text(sql_query))
            rows = result.fetchall()
            columns = result.keys()
        
        # Convert to list of dicts
        data = [dict(zip(columns, row)) for row in rows]
        
        return {
            "sql_query": sql_query,
            "results": data,
            "row_count": len(data)
        }
    
    except SQLAlchemyError as e:
        raise HTTPException(status_code=400, detail=f"SQL Error: {str(e)}")
    except requests.exceptions.Timeout:
        raise HTTPException(status_code=504, detail="Ollama request timed out")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/execute-sql")
async def execute_sql(sql: str = Form(...)):
    """Execute raw SQL query"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql))
            
            # Check if it's a SELECT query
            if result.returns_rows:
                rows = result.fetchall()
                columns = result.keys()
                data = [dict(zip(columns, row)) for row in rows]
                return {
                    "sql_query": sql,
                    "results": data,
                    "row_count": len(data)
                }
            else:
                conn.commit()
                return {
                    "sql_query": sql,
                    "message": "Query executed successfully"
                }
    
    except SQLAlchemyError as e:
        raise HTTPException(status_code=400, detail=f"SQL Error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/table/{table_name}")
def get_table_data(table_name: str, limit: int = 100):
    """Get data from specific table"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT * FROM {table_name} LIMIT {limit}"))
            rows = result.fetchall()
            columns = result.keys()
        
        data = [dict(zip(columns, row)) for row in rows]
        
        return {
            "table_name": table_name,
            "data": data,
            "row_count": len(data)
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/table/{table_name}")
def delete_table(table_name: str):
    """Delete a table"""
    try:
        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
            conn.commit()
        
        return {"message": f"Table {table_name} deleted successfully"}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/config")
def get_config():
    """Get current configuration (for debugging)"""
    return {
        "environment": settings.ENVIRONMENT,
        "ollama_url": settings.OLLAMA_URL,
        "ollama_model": settings.OLLAMA_MODEL,
        "backend_port": settings.BACKEND_PORT,
        "database_connected": True
    }


@app.get("/validate-data")
def validate_data():
    """Check data consistency between HIS and RIS tables"""
    try:
        with engine.connect() as conn:
            # Query 1: Count total records
            his_count = conn.execute(text("SELECT COUNT(*) FROM his")).scalar()
            ris_count = conn.execute(text("SELECT COUNT(*) FROM ris")).scalar()
            
            # Query 2: Count unique bill_id/patient_id
            his_unique = conn.execute(text("SELECT COUNT(DISTINCT bill_id) FROM his")).scalar()
            ris_unique = conn.execute(text("SELECT COUNT(DISTINCT patient_id) FROM ris")).scalar()
            
            # Query 3: Find missing bill_ids in RIS
            missing_in_ris = conn.execute(text("""
                SELECT DISTINCT h.bill_id 
                FROM his h 
                LEFT JOIN ris r ON h.bill_id = r.patient_id 
                WHERE r.patient_id IS NULL
            """)).fetchall()
            
            # Query 4: Find missing patient_ids in HIS
            missing_in_his = conn.execute(text("""
                SELECT DISTINCT r.patient_id 
                FROM ris r 
                LEFT JOIN his h ON r.patient_id = h.bill_id 
                WHERE h.bill_id IS NULL
            """)).fetchall()
            
            # Query 5: Count services per bill_id in HIS
            his_services = conn.execute(text("""
                SELECT bill_id, COUNT(*) as service_count, patient_name
                FROM his 
                GROUP BY bill_id, patient_name
                ORDER BY service_count DESC
            """)).fetchall()
            
            # Query 6: Count entries per patient_id in RIS
            ris_entries = conn.execute(text("""
                SELECT patient_id, COUNT(*) as entry_count, patient
                FROM ris 
                GROUP BY patient_id, patient
                ORDER BY entry_count DESC
            """)).fetchall()
            
            # Query 7: Compare counts - patients with mismatched service counts
            mismatched = conn.execute(text("""
                SELECT 
                    h.bill_id,
                    h.patient_name as his_name,
                    COUNT(DISTINCT h.id) as his_services,
                    r.patient as ris_name,
                    COUNT(DISTINCT r.id) as ris_services
                FROM his h
                LEFT JOIN ris r ON h.bill_id = r.patient_id
                GROUP BY h.bill_id, h.patient_name, r.patient
                HAVING COUNT(DISTINCT h.id) != COUNT(DISTINCT r.id)
            """)).fetchall()
            
            return {
                "summary": {
                    "his_total_records": his_count,
                    "ris_total_records": ris_count,
                    "his_unique_bill_ids": his_unique,
                    "ris_unique_patient_ids": ris_unique,
                    "missing_in_ris_count": len(missing_in_ris),
                    "missing_in_his_count": len(missing_in_his),
                    "mismatched_count": len(mismatched)
                },
                "missing_in_ris": [row[0] for row in missing_in_ris],
                "missing_in_his": [row[0] for row in missing_in_his],
                "his_service_counts": [
                    {"bill_id": row[0], "services": row[1], "patient": row[2]} 
                    for row in his_services[:50]
                ],
                "ris_entry_counts": [
                    {"patient_id": row[0], "entries": row[1], "patient": row[2]} 
                    for row in ris_entries[:50]
                ],
                "mismatched_records": [
                    {
                        "bill_id": row[0],
                        "his_name": row[1],
                        "his_services": row[2],
                        "ris_name": row[3],
                        "ris_services": row[4]
                    } 
                    for row in mismatched
                ]
            }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/records-comparison")
def records_comparison():
    """Query 1: Total Records Comparison"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    'HIS' as source,
                    COUNT(*) as total_records,
                    COUNT(DISTINCT bill_id) as unique_ids
                FROM his
                UNION ALL
                SELECT 
                    'RIS' as source,
                    COUNT(*) as total_records,
                    COUNT(DISTINCT patient_id) as unique_ids
                FROM ris
            """))
            rows = result.fetchall()
            columns = result.keys()
        
        return {"data": [dict(zip(columns, row)) for row in rows]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/services-per-patient")
def services_per_patient():
    """Query 2: Services per Patient (HIS)"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    bill_id,
                    patient_name,
                    COUNT(*) as service_count,
                    STRING_AGG(DISTINCT service_description, ', ') as services
                FROM his
                GROUP BY bill_id, patient_name
                ORDER BY service_count DESC
                LIMIT 100
            """))
            rows = result.fetchall()
            columns = result.keys()
        
        return {"data": [dict(zip(columns, row)) for row in rows]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/missing-in-ris")
def missing_in_ris():
    """Query 3: Missing Records in RIS"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    h.bill_id,
                    h.patient_name,
                    COUNT(*) as his_services
                FROM his h
                LEFT JOIN ris r ON h.bill_id = r.patient_id
                WHERE r.patient_id IS NULL
                GROUP BY h.bill_id, h.patient_name
            """))
            rows = result.fetchall()
            columns = result.keys()
        
        return {"data": [dict(zip(columns, row)) for row in rows]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/service-mismatch")
def service_mismatch():
    """Query 4: Service Count Mismatch"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    h.bill_id,
                    h.patient_name as his_name,
                    COUNT(DISTINCT h.id) as his_count,
                    r.patient as ris_name,
                    COUNT(DISTINCT r.id) as ris_count,
                    ABS(COUNT(DISTINCT h.id) - COUNT(DISTINCT r.id)) as difference
                FROM his h
                LEFT JOIN ris r ON h.bill_id = r.patient_id
                GROUP BY h.bill_id, h.patient_name, r.patient
                HAVING COUNT(DISTINCT h.id) != COUNT(DISTINCT r.id)
                ORDER BY difference DESC
            """))
            rows = result.fetchall()
            columns = result.keys()
        
        return {"data": [dict(zip(columns, row)) for row in rows]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/daily-trends")
def daily_trends():
    """Query 5: Daily Service Trends"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    bill_date as date,
                    COUNT(*) as total_services,
                    COUNT(DISTINCT bill_id) as unique_patients,
                    COUNT(DISTINCT service_description) as service_types
                FROM his
                GROUP BY bill_date
                ORDER BY bill_date DESC
            """))
            rows = result.fetchall()
            columns = result.keys()
        
        return {"data": [dict(zip(columns, row)) for row in rows]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/top-services")
def top_services():
    """Query 6: Top Services"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    service_description,
                    COUNT(*) as count,
                    COUNT(DISTINCT bill_id) as unique_patients
                FROM his
                GROUP BY service_description
                ORDER BY count DESC
                LIMIT 20
            """))
            rows = result.fetchall()
            columns = result.keys()
        
        return {"data": [dict(zip(columns, row)) for row in rows]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
