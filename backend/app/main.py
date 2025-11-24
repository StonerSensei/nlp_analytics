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
engine = create_engine(settings.database_url)

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
        upload_dir = "/app/uploads"
        os.makedirs(upload_dir, exist_ok=True)
        file_path = os.path.join(upload_dir, file.filename)
        
        with open(file_path, "wb") as f:
            content = await file.read()
            f.write(content)
        
        df = pd.read_csv(file_path, skiprows=skip_rows)
        
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('/', '_').str.replace('.', '_')
        
        for col in df.columns:
            df[col] = df[col].astype(str).replace('nan', None)
        
        df.insert(0, 'id', range(1, len(df) + 1))
        
        # Drop table if exists and create new one
        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
            conn.commit()
        
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

def clean_sql_query(sql_text: str) -> str:
    """Clean and extract SQL query from LLM response"""
    
    sql_text = sql_text.replace("```sql", "").replace("```", "")
    
    prefixes_to_remove = [
        "SQL Query:",
        "Here's the SQL query:",
        "The SQL query is:",
        "Query:",
        "SELECT",  
    ]
    
    original = sql_text
    for prefix in prefixes_to_remove:
        if sql_text.strip().startswith(prefix) and prefix != "SELECT":
            sql_text = sql_text.strip()[len(prefix):].strip()
    
    if original.strip().upper().startswith("SELECT") and not sql_text.strip().upper().startswith("SELECT"):
        sql_text = "SELECT " + sql_text
    
    sql_text = sql_text.rstrip(';').strip()
    
    if '\n\n' in sql_text:
        lines = sql_text.split('\n\n')
        for line in lines:
            clean_line = line.strip()
            if clean_line.upper().startswith('SELECT'):
                return clean_line
    
    if not sql_text.upper().startswith('SELECT'):
        for line in sql_text.split('\n'):
            if line.strip().upper().startswith('SELECT'):
                return line.strip()
    
    return sql_text

def validate_and_fix_sql(sql_query: str) -> tuple[str, list[str]]:
    """Validate SQL query and fix common mistakes. Returns (fixed_query, warnings)"""
    warnings = []
    fixed_query = sql_query
    
    if 'JOIN ris ON' in sql_query.upper():
        if 'ris.id' in sql_query.lower() or 'ON his.bill_id = ris.id' in sql_query.lower():
            warnings.append("Fixed: Changed 'ris.id' to 'ris.patient_id' in JOIN condition")
            fixed_query = fixed_query.replace('ris.id', 'ris.patient_id')
            fixed_query = fixed_query.replace('RIS.id', 'ris.patient_id')
        
        if 'his.id' in sql_query.lower() and 'JOIN' in sql_query.upper():
            warnings.append("Fixed: Changed 'his.id' to 'his.bill_id' in JOIN condition")
            import re
            fixed_query = re.sub(r'his\.id\s*=\s*ris', 'his.bill_id = ris', fixed_query, flags=re.IGNORECASE)
    
    if any(col in sql_query for col in ['bill_id', 'patient_id', 'patient_mobile_no']):
        if '::bigint' in sql_query.lower() or '::integer' in sql_query.lower():
            warnings.append("Removed incorrect type casting - these columns are TEXT type")
            fixed_query = fixed_query.replace('::bigint', '').replace('::BIGINT', '')
            fixed_query = fixed_query.replace('::integer', '').replace('::INTEGER', '')
    
    return fixed_query, warnings

@app.post("/query")
async def natural_language_query(query: str = Form(...)):
    """Convert natural language to SQL and execute"""
    try:
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        
        schema_info = ""
        for table in tables:
            columns = inspector.get_columns(table)
            col_definitions = []
            
            for col in columns:
                col_type = "TEXT" if col['name'] != 'id' else "BIGINT"
                col_definitions.append(f"  {col['name']} {col_type}")
            
            schema_info += f"CREATE TABLE {table} (\n" + ",\n".join(col_definitions) + "\n);\n\n"
        
        prompt = f"""### Task
Generate a SQL query to answer the following question: `{query}`

### Database Schema
{schema_info}

### Important Notes
- To join HIS and RIS tables, use: his.bill_id = ris.patient_id
- his.id and ris.id are auto-generated primary keys, do NOT use them for joins
- All columns except id are TEXT type

### Answer
Given the database schema, here is the SQL query that answers `{query}`:
```sql
"""

        print(f"Prompt for sqlcoder:\n{prompt}\n")

        max_retries = 2
        timeout_seconds = 120
        
        ollama_response = None
        last_error = None
        
        for attempt in range(max_retries):
            try:
                print(f"[Attempt {attempt + 1}/{max_retries}] Calling Ollama...")
                
                ollama_response = requests.post(
                    f"{settings.OLLAMA_URL}/api/generate",
                    json={
                        "model": settings.OLLAMA_MODEL,
                        "prompt": prompt,
                        "stream": False,
                        "options": {
                            "temperature": 0.1,
                            "num_predict": 1000, 
                            "stop": ["```", "\n\n\n", "###"] 
                        }
                    },
                    timeout=timeout_seconds
                )
                
                print(f"[Attempt {attempt + 1}] Response status: {ollama_response.status_code}")
                print(f"[Attempt {attempt + 1}] Response preview: {ollama_response.text[:200]}")
                
                if ollama_response.status_code == 200:
                    break 
                else:
                    last_error = f"Status {ollama_response.status_code}: {ollama_response.text}"
                    
            except requests.exceptions.Timeout:
                last_error = f"Timeout after {timeout_seconds} seconds"
                print(f"[Attempt {attempt + 1}] {last_error}")
                if attempt < max_retries - 1:
                    continue  
                else:
                    raise HTTPException(
                        status_code=504, 
                        detail=f"Ollama request timed out after {timeout_seconds} seconds. The model may be overloaded or the query is too complex. Try simplifying your question."
                    )
            except requests.exceptions.ConnectionError as e:
                last_error = f"Connection error: {str(e)}"
                print(f"[Attempt {attempt + 1}] {last_error}")
                raise HTTPException(
                    status_code=503,
                    detail="Cannot connect to Ollama service. Please ensure Ollama is running."
                )
            except Exception as e:
                last_error = f"Unexpected error: {str(e)}"
                print(f"[Attempt {attempt + 1}] {last_error}")
        
        if not ollama_response or ollama_response.status_code != 200:
            raise HTTPException(
                status_code=500, 
                detail=f"Ollama error: {last_error}"
            )

        response_json = ollama_response.json()
        print(f"Full Ollama response: {response_json}")
        
        raw_response = response_json.get("response", "").strip()
        
        if "response" not in response_json:
            raise HTTPException(
                status_code=500, 
                detail=f"Ollama response missing 'response' field. Full response: {response_json}"
            )
        
        if not raw_response:
            if "error" in response_json:
                raise HTTPException(
                    status_code=500,
                    detail=f"Ollama error: {response_json['error']}"
                )
            
            if response_json.get('eval_count', 0) <= 1:
                raise HTTPException(
                    status_code=500,
                    detail=f"Model stopped immediately after generating only {response_json.get('eval_count', 0)} token(s). This usually means the prompt format is incorrect for the model. Try using 'qwen2.5:7b' or 'llama3:8b' instead of '{settings.OLLAMA_MODEL}'."
                )
            
            raise HTTPException(
                status_code=400, 
                detail=f"LLM returned an empty response. Full JSON: {response_json}"
            )
        
        print(f"Raw LLM response: {raw_response}")
        
        sql_query = clean_sql_query(raw_response)
        
        if not sql_query or not sql_query.upper().startswith('SELECT'):
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid SQL query generated. Raw response: {raw_response[:200]}"
            )
        
        sql_query, warnings = validate_and_fix_sql(sql_query)
        
        with engine.connect() as conn:
            conn.execute(text("SET statement_timeout = 30000"))
            
            result = conn.execute(text(sql_query))
            rows = result.fetchall()
            columns = result.keys()
        
        data = [dict(zip(columns, row)) for row in rows]
        
        response_data = {
            "sql_query": sql_query,
            "results": data,
            "row_count": len(data)
        }
        
        if warnings:
            response_data["warnings"] = warnings
            response_data["original_query"] = raw_response
        
        return response_data
    
    except SQLAlchemyError as e:
        raise HTTPException(status_code=400, detail=f"SQL Error: {str(e)}")
    except requests.exceptions.Timeout:
        raise HTTPException(
            status_code=504, 
            detail="Request timed out. Try simplifying your query or check if Ollama is responding."
        )
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=503, detail=f"Ollama service error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/execute-sql")
async def execute_sql(sql: str = Form(...)):
    """Execute raw SQL query"""
    try:
        with engine.connect() as conn:
            
            conn.execute(text("SET statement_timeout = 30000"))
            
            result = conn.execute(text(sql))
            
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

@app.get("/schema-info")
def get_schema_info():
    """Get detailed schema information with relationships"""
    try:
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        
        schema = {}
        for table in tables:
            columns = inspector.get_columns(table)
            
            with engine.connect() as conn:
                sample = conn.execute(text(f"SELECT * FROM {table} LIMIT 3"))
                sample_rows = [dict(zip(sample.keys(), row)) for row in sample.fetchall()]
            
            schema[table] = {
                "columns": [
                    {
                        "name": col['name'],
                        "type": str(col['type']),
                        "nullable": col['nullable']
                    }
                    for col in columns
                ],
                "sample_data": sample_rows
            }
        
        relationships = {
            "his_to_ris": {
                "description": "HIS table relates to RIS table",
                "join_condition": "his.bill_id = ris.patient_id",
                "note": "Both are TEXT type. DO NOT use his.id or ris.id for joining!"
            }
        }
        
        return {
            "tables": schema,
            "relationships": relationships,
            "important_notes": [
                "his.id and ris.id are auto-generated integer primary keys - NOT for joining",
                "Use his.bill_id = ris.patient_id to join the tables",
                "All columns except 'id' are TEXT type",
                "Phone numbers, dates, and IDs are stored as text strings"
            ]
        }
    
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

@app.post("/test-ollama")
async def test_ollama(prompt: str = Form("Say hello")):
    """Test Ollama connectivity and response"""
    try:
        response = requests.post(
            f"{settings.OLLAMA_URL}/api/generate",
            json={
                "model": settings.OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.1,
                    "num_predict": 100
                }
            },
            timeout=30
        )
        
        return {
            "status_code": response.status_code,
            "response_json": response.json(),
            "raw_text": response.text
        }
    except Exception as e:
        return {
            "error": str(e),
            "type": type(e).__name__
        }

@app.get("/ollama-health")
def check_ollama():
    """Check if Ollama is responding"""
    try:
        response = requests.get(f"{settings.OLLAMA_URL}/api/tags", timeout=5)
        if response.status_code == 200:
            models = response.json().get('models', [])
            return {
                "status": "healthy",
                "available_models": [m['name'] for m in models],
                "configured_model": settings.OLLAMA_MODEL
            }
        else:
            return {"status": "unhealthy", "error": f"Status code: {response.status_code}"}
    except Exception as e:
        return {"status": "unreachable", "error": str(e)}

@app.get("/validate-data")
def validate_data():
    """Check data consistency between HIS and RIS tables"""
    try:
        with engine.connect() as conn:
            # Count total records
            his_count = conn.execute(text("SELECT COUNT(*) FROM his")).scalar()
            ris_count = conn.execute(text("SELECT COUNT(*) FROM ris")).scalar()
            
            # Count unique bill_id/patient_id
            his_unique = conn.execute(text("SELECT COUNT(DISTINCT bill_id) FROM his")).scalar()
            ris_unique = conn.execute(text("SELECT COUNT(DISTINCT patient_id) FROM ris")).scalar()
            
            # Find missing bill_ids in RIS
            missing_in_ris = conn.execute(text("""
                SELECT DISTINCT h.bill_id 
                FROM his h 
                LEFT JOIN ris r ON h.bill_id = r.patient_id 
                WHERE r.patient_id IS NULL
            """)).fetchall()
            
            # Find missing patient_ids in HIS
            missing_in_his = conn.execute(text("""
                SELECT DISTINCT r.patient_id 
                FROM ris r 
                LEFT JOIN his h ON r.patient_id = h.bill_id 
                WHERE h.bill_id IS NULL
            """)).fetchall()
            
            # Count services per bill_id in HIS
            his_services = conn.execute(text("""
                SELECT bill_id, COUNT(*) as service_count, patient_name
                FROM his 
                GROUP BY bill_id, patient_name
                ORDER BY service_count DESC
            """)).fetchall()
            
            # Count entries per patient_id in RIS
            ris_entries = conn.execute(text("""
                SELECT patient_id, COUNT(*) as entry_count, patient
                FROM ris 
                GROUP BY patient_id, patient
                ORDER BY entry_count DESC
            """)).fetchall()
            
            # Compare counts - patients with mismatched service counts
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
@app.get("/analytics/service-comparison")
def service_comparison(patient_name: str = ""):
    """Query 7: Service Comparison - HIS vs RIS side by side"""
    try:
        with engine.connect() as conn:
            # Base query - join HIS and RIS on bill_id = patient_id
            base_query = """
                SELECT 
                    COALESCE(h.bill_id, r.patient_id) as id,
                    COALESCE(h.patient_name, r.patient) as patient_name,
                    h.patient_name as his_patient_name,
                    r.patient as ris_patient_name,
                    STRING_AGG(DISTINCT h.service_description, ' | ') as his_services,
                    STRING_AGG(DISTINCT r.test_name, ' | ') as ris_services,
                    COUNT(DISTINCT h.id) as his_service_count,
                    COUNT(DISTINCT r.id) as ris_service_count
                FROM his h
                FULL OUTER JOIN ris r ON h.bill_id = r.patient_id
            """
            
            # Add filter if patient name provided
            if patient_name:
                base_query += """
                WHERE LOWER(h.patient_name) LIKE LOWER(:pattern) 
                   OR LOWER(r.patient) LIKE LOWER(:pattern)
                """
                result = conn.execute(
                    text(base_query + """
                        GROUP BY h.bill_id, r.patient_id, h.patient_name, r.patient
                        ORDER BY patient_name
                    """),
                    {"pattern": f"%{patient_name}%"}
                )
            else:
                result = conn.execute(text(base_query + """
                    GROUP BY h.bill_id, r.patient_id, h.patient_name, r.patient
                    ORDER BY patient_name
                    LIMIT 100
                """))
            
            rows = result.fetchall()
            columns = result.keys()
        
        return {"data": [dict(zip(columns, row)) for row in rows]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/analytics/patient-search")
def patient_search(query: str):
    """Search for patients by name across both tables"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT DISTINCT 
                    COALESCE(h.patient_name, r.patient) as patient_name,
                    COALESCE(h.bill_id, r.patient_id) as patient_id
                FROM his h
                FULL OUTER JOIN ris r ON h.bill_id = r.patient_id
                WHERE LOWER(h.patient_name) LIKE LOWER(:pattern)
                   OR LOWER(r.patient) LIKE LOWER(:pattern)
                ORDER BY patient_name
                LIMIT 50
            """), {"pattern": f"%{query}%"})
            
            rows = result.fetchall()
            columns = result.keys()
        
        return {"data": [dict(zip(columns, row)) for row in rows]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
