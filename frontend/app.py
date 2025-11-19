import streamlit as st
import requests
import pandas as pd
import os
import json
from datetime import datetime


BACKEND_URL = os.getenv("BACKEND_API_URL", "http://backend:8000")

st.set_page_config(
    page_title="Hospital Analytics",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #666;
    }
    .success-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        color: #155724;
    }
    .error-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #f8d7da;
        border: 1px solid #f5c6cb;
        color: #721c24;
    }
    .info-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #d1ecf1;
        border: 1px solid #bee5eb;
        color: #0c5460;
    }
    .stButton>button {
        width: 100%;
    }
</style>
""", unsafe_allow_html=True)

st.markdown('<p class="main-header">Hospital Analytics Platform</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Natural Language to SQL Query System</p>', unsafe_allow_html=True)

with st.sidebar:
    st.header("Navigation")
    page = st.radio(
        "Go to:",
        ["Home", "Upload CSV", "Query Data", "View Tables", "Analytics"],
        key="navigation"
    )
    
    st.markdown("---")
    
    st.subheader("System Status")
    try:
        response = requests.get(f"{BACKEND_URL}/health", timeout=5)
        if response.status_code == 200:
            health = response.json()
            st.success("Backend Online")
            
            if health.get("database") == "connected":
                st.success("Database Connected")
            else:
                st.error("Database Disconnected")
            
            if health.get("ollama") == "connected":
                st.success("AI Model Ready")
            else:
                st.warning("AI Model Not Ready")
        else:
            st.error("Backend Error")
    except Exception as e:
        st.error("Cannot reach backend")
        st.caption(f"Error: {str(e)}")
    
    st.markdown("---")
    st.caption("Powered by Ollama & SQLCoder")

if page == "Home":
    st.header("Welcome to Hospital Analytics")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.info("**Upload Data**\n\nUpload CSV files to automatically create database tables")
    
    with col2:
        st.info("**Query**\n\nAsk questions in natural language and get instant answers")
    
    with col3:
        st.info("**Analyze**\n\nVisualize your data with auto-generated charts")
    
    st.markdown("---")
    st.subheader("Quick Start Guide")
    
    st.markdown("""
    ### 1 Upload Your Data
    - Go to **Upload CSV** page
    - Select your CSV file
    - Table will be automatically created
    
    ### 2 Ask Questions
    - Go to **Query Data** page
    - Type your question in plain English
    - Get SQL query and results instantly
    
    ### 3 Explore Tables
    - Go to **View Tables** page
    - Browse all your tables
    - View sample data and statistics
    
    ### 4 Analyze
    - Go to **Analytics** page
    - Access Metabase dashboard
    - Create custom visualizations
    """)
    
    st.markdown("---")
    
    st.subheader("Example Questions You Can Ask")
    
    examples = [
        "How many employees are there?",
        "Show me the average salary by department",
        "List the top 5 highest paid employees",
        "How many employees have more than 3 years of experience?",
        "What is the total salary expense by department?",
        "Show me employees hired in 2020"
    ]
    
    cols = st.columns(2)
    for i, example in enumerate(examples):
        with cols[i % 2]:
            st.code(example, language=None)

elif page == "Upload CSV":
    st.header("Upload CSV File - Wizard")
    
    # Initialize wizard state
    if 'wizard_step' not in st.session_state:
        st.session_state.wizard_step = 1
    if 'wizard_data' not in st.session_state:
        st.session_state.wizard_data = {}
    
    # Progress indicator
    steps = ["Upload", "Parse", "Schema", "Primary Key", "Foreign Keys", "Confirm"]
    current_step = st.session_state.wizard_step
    
    progress_cols = st.columns(len(steps))
    for i, step in enumerate(steps):
        with progress_cols[i]:
            if i + 1 < current_step:
                st.success(f"✓ {step}")
            elif i + 1 == current_step:
                st.info(f"→ **{step}**")
            else:
                st.text(f"○ {step}")
    
    st.markdown("---")
    
    # Helper functions for navigation
    def next_step():
        st.session_state.wizard_step += 1
    
    def prev_step():
        st.session_state.wizard_step -= 1
    
    def reset_wizard():
        st.session_state.wizard_step = 1
        st.session_state.wizard_data = {}
    
    # STEP 1: Upload File
    if current_step == 1:
        st.subheader("Step 1: Upload CSV File")
        
        uploaded_file = st.file_uploader("Choose a CSV file", type=['csv'], key="wizard_upload")
        
        if uploaded_file is not None:
            st.session_state.wizard_data['file'] = uploaded_file
            st.session_state.wizard_data['filename'] = uploaded_file.name
            
            st.success(f"File selected: {uploaded_file.name}")
            st.caption(f"Size: {uploaded_file.size:,} bytes")
            
            if st.button("Next: Analyze Structure", type="primary"):
                with st.spinner("Analyzing file..."):
                    try:
                        uploaded_file.seek(0)
                        files = {"file": (uploaded_file.name, uploaded_file.read(), "text/csv")}
                        data = {"preview_lines": 20}
                        
                        response = requests.post(
                            f"{BACKEND_URL}/api/upload/preview",
                            files=files,
                            data=data,
                            timeout=30
                        )
                        
                        if response.status_code == 200:
                            result = response.json()
                            st.session_state.wizard_data['analysis'] = result['analysis']
                            next_step()
                            st.rerun()
                        else:
                            st.error(f"Analysis failed: {response.text}")
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
        else:
            st.info("Please upload a CSV file to begin")
    
    # STEP 2: Configure Parsing
    elif current_step == 2:
        st.subheader("Step 2: Configure CSV Parsing")
        
        analysis = st.session_state.wizard_data.get('analysis', {})
        
        if not analysis:
            st.error("Analysis data missing. Please restart.")
            if st.button("Restart"):
                reset_wizard()
                st.rerun()
            st.stop()
        
        # Show detection confidence
        confidence = analysis.get('confidence', 0)
        if confidence >= 80:
            st.success(f"High confidence detection: {confidence:.0f}%")
        elif confidence >= 60:
            st.warning(f"Medium confidence: {confidence:.0f}% - Please verify")
        else:
            st.error(f"Low confidence: {confidence:.0f}% - Manual adjustment recommended")
        
        st.caption(f"Reasoning: {analysis.get('reasoning', 'N/A')}")
        
        # Show preview
        st.markdown("#### Raw File Preview")
        preview_text = ""
        for i, line in enumerate(analysis.get('preview', [])[:20]):
            if i == analysis.get('detected_header_row', 0):
                preview_text += f">>> [{i}] {line}\n"
            else:
                preview_text += f"    [{i}] {line}\n"
        
        st.text_area("First 20 lines", preview_text, height=300, disabled=True)
        
        # Manual adjustment
        col1, col2 = st.columns(2)
        
        with col1:
            header_row = st.number_input(
                "Header row number",
                min_value=0,
                max_value=19,
                value=analysis.get('detected_header_row', 0),
                help="Which row contains column names?"
            )
        
        with col2:
            if header_row > 0:
                st.info(f"Will skip rows: 0 to {header_row - 1}")
        
        st.session_state.wizard_data['header_row'] = header_row
        
        # Navigation
        col1, col2 = st.columns(2)
        with col1:
            if st.button("← Back"):
                prev_step()
                st.rerun()
        with col2:
            if st.button("Next: Generate Schema →", type="primary"):
                with st.spinner("Generating schema..."):
                    try:
                        uploaded_file = st.session_state.wizard_data['file']
                        uploaded_file.seek(0)
                        
                        skip_rows_str = ",".join(map(str, range(header_row))) if header_row > 0 else None
                        
                        files = {"file": (uploaded_file.name, uploaded_file.read(), "text/csv")}
                        data = {
                            "header_row": header_row,
                            "skip_rows": skip_rows_str
                        }
                        
                        response = requests.post(
                            f"{BACKEND_URL}/api/upload/analyze",
                            files=files,
                            data=data,
                            timeout=30
                        )
                        
                        if response.status_code == 200:
                            st.session_state.wizard_data['schema'] = response.json()
                            next_step()
                            st.rerun()
                        else:
                            st.error(f"Schema generation failed: {response.text}")
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
    
    # STEP 3: Review Schema
    elif current_step == 3:
        st.subheader("Step 3: Review Detected Schema")
        
        schema = st.session_state.wizard_data.get('schema', {})
        
        if not schema:
            st.error("Schema data missing")
            if st.button("Restart"):
                reset_wizard()
                st.rerun()
            st.stop()
        
        # Table info
        col1, col2, col3 = st.columns(3)
        with col1:
            table_name = st.text_input("Table name", value=schema.get('table_name', ''))
            st.session_state.wizard_data['table_name'] = table_name
        with col2:
            st.metric("Rows", schema.get('row_count', 0))
        with col3:
            st.metric("Columns", len(schema.get('columns', [])))
        
        # Show columns
        st.markdown("#### Detected Columns")
        if schema.get('columns'):
            cols_df = pd.DataFrame(schema['columns'])[['name', 'sql_type', 'nullable', 'unique']]
            st.dataframe(cols_df, use_container_width=True)
        
        # Sample data
        with st.expander("View Sample Data"):
            if schema.get('sample_data'):
                st.dataframe(pd.DataFrame(schema['sample_data']))
        
        # Navigation
        col1, col2 = st.columns(2)
        with col1:
            if st.button("← Back"):
                prev_step()
                st.rerun()
        with col2:
            if st.button("Next: Select Primary Key →", type="primary"):
                # Get column statistics
                with st.spinner("Analyzing columns for primary key selection..."):
                    try:
                        uploaded_file = st.session_state.wizard_data['file']
                        uploaded_file.seek(0)
                        
                        header_row = st.session_state.wizard_data.get('header_row', 0)
                        skip_rows_str = ",".join(map(str, range(header_row))) if header_row > 0 else None
                        
                        files = {"file": (uploaded_file.name, uploaded_file.read(), "text/csv")}
                        data = {
                            "header_row": header_row,
                            "skip_rows": skip_rows_str
                        }
                        
                        response = requests.post(
                            f"{BACKEND_URL}/api/upload/column-stats",
                            files=files,
                            data=data,
                            timeout=30
                        )
                        
                        if response.status_code == 200:
                            st.session_state.wizard_data['column_stats'] = response.json()
                            next_step()
                            st.rerun()
                        else:
                            st.error(f"Failed to get column stats: {response.text}")
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
    
    # STEP 4: Select Primary Key
    elif current_step == 4:
        st.subheader("Step 4: Select Primary Key")
        
        st.info("""
        **Primary Key Options:**
        - **Auto-generated ID (Recommended):** System adds an automatic row number
        - **Select existing column:** Choose a column with unique values
        - **No primary key:** Not recommended for most cases
        """)
        
        column_stats = st.session_state.wizard_data.get('column_stats', {})
        schema = st.session_state.wizard_data.get('schema', {})
        
        if not column_stats or not schema:
            st.error("Missing data")
            if st.button("Restart"):
                reset_wizard()
                st.rerun()
            st.stop()
        
        stats = column_stats.get('column_stats', [])
        
        # Show column suitability analysis
        st.markdown("#### Column Suitability Analysis")
        
        suitable_columns = []
        
        # Create expandable section for column details
        with st.expander("View detailed column analysis", expanded=False):
            for stat in stats:
                col1, col2, col3, col4 = st.columns([3, 2, 2, 2])
                
                with col1:
                    st.text(stat['name'])
                with col2:
                    if stat['suitable_for_pk']:
                        st.success("✓ Suitable")
                        suitable_columns.append(stat['name'])
                    else:
                        st.error("✗ Not suitable")
                with col3:
                    st.caption(f"{stat['uniqueness_percent']}% unique")
                with col4:
                    if stat['has_nulls']:
                        st.caption(f"⚠ {stat['null_count']} nulls")
                    else:
                        st.caption("✓ No nulls")
        
        # Show summary
        if suitable_columns:
            st.success(f"Found {len(suitable_columns)} suitable column(s) for primary key")
        else:
            st.warning("No suitable columns found. Auto-generated ID is recommended.")
        
        st.markdown("---")
        
        # Primary key selector
        detected_pk = schema.get('primary_key', '')
        
        # Build options list
        pk_options = [
            "🔑 Auto-generated ID (Recommended)",
            "⊘ No primary key"
        ]
        
        # Add suitable columns
        if suitable_columns:
            pk_options.insert(1, "--- Existing Columns ---")
            pk_options.extend(suitable_columns)
        
        # Determine default selection
        default_index = 0  # Default to auto-generated
        if detected_pk and detected_pk in suitable_columns:
            # If a suitable column was auto-detected, select it
            default_index = pk_options.index(detected_pk)
        
        selected_pk = st.selectbox(
            "Select primary key strategy",
            options=pk_options,
            index=default_index,
            help="Choose how to uniquely identify each row"
        )
        
        if selected_pk == "🔑 Auto-generated ID (Recommended)":
            st.session_state.wizard_data['primary_key'] = "__Auto__" 
            st.success("✓ An auto-incrementing 'id' column will be added to your table")
            st.caption("Each row will get a unique sequential number (1, 2, 3, ...)")
            
        elif selected_pk == "⊘ No primary key":
            st.session_state.wizard_data['primary_key'] = "__None__"  
            st.error("⚠ Not recommended: Table will have no primary key")
            st.caption("This may cause issues with data integrity and updates")
            
        elif selected_pk == "--- Existing Columns ---":
            st.info("Please select a specific column or use auto-generated ID")
            
        else:
            st.session_state.wizard_data['primary_key'] = selected_pk
            st.success(f"✓ Primary key: **{selected_pk}**")
            
            selected_stat = next((s for s in stats if s['name'] == selected_pk), None)
            if selected_stat:
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Unique Values", f"{selected_stat['uniqueness_percent']}%")
                with col2:
                    st.metric("Null Count", selected_stat['null_count'])
                with col3:
                    st.metric("Total Rows", selected_stat['total_rows'])
                
                if selected_stat['suitable_for_pk']:
                    st.success(f"✓ Primary key: **{selected_pk}**")
                else:
                    st.warning(f"⚠ **{selected_pk}** may not be suitable as primary key")
                    if selected_stat['has_nulls']:
                        st.caption("Contains null values")
                    if not selected_stat['is_unique']:
                        st.caption("Contains duplicate values")
        
        st.markdown("---")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("← Back"):
                prev_step()
                st.rerun()
        with col2:
            if st.button("Next: Foreign Keys →", type="primary"):
                next_step()
                st.rerun()

    

    elif current_step == 5:
        st.subheader("Step 5: Configure Foreign Keys (Optional)")
        
        st.info("Foreign keys link this table to other tables in your database.")
        
        schema = st.session_state.wizard_data.get('schema', {})
        
        if 'foreign_keys' not in st.session_state.wizard_data:
            st.session_state.wizard_data['foreign_keys'] = []
        
        foreign_keys = st.session_state.wizard_data['foreign_keys']
        
        column_names = [col['name'] for col in schema.get('columns', [])]
        
        st.markdown("#### Current Foreign Keys")
        
        if not foreign_keys:
            st.caption("No foreign keys defined")
        else:
            for i, fk in enumerate(foreign_keys):
                col1, col2, col3, col4 = st.columns([3, 3, 3, 1])
                
                with col1:
                    st.text(f"{fk['column']}")
                with col2:
                    st.text(f"→ {fk['ref_table']}")
                with col3:
                    st.text(f".{fk['ref_column']}")
                with col4:
                    if st.button("🗑️", key=f"remove_fk_{i}"):
                        foreign_keys.pop(i)
                        st.rerun()
        
        st.markdown("---")
        st.markdown("#### Add Foreign Key")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            fk_column = st.selectbox(
                "Column in this table",
                options=[""] + column_names,
                key="new_fk_column"
            )
        
        with col2:
            fk_ref_table = st.text_input(
                "References table",
                placeholder="e.g., patients",
                key="new_fk_ref_table"
            )
        
        with col3:
            fk_ref_column = st.text_input(
                "References column",
                value="id",
                placeholder="e.g., id",
                key="new_fk_ref_column"
            )
        
        if st.button("Validate & Add Foreign Key"):
            if not fk_column or not fk_ref_table or not fk_ref_column:
                st.error("All fields are required")
            else:
                with st.spinner("Validating foreign key..."):
                    try:
                        data = {
                            "ref_table": fk_ref_table,
                            "ref_column": fk_ref_column
                        }
                        
                        response = requests.post(
                            f"{BACKEND_URL}/api/upload/validate-foreign-key",
                            data=data,
                            timeout=10
                        )
                        
                        if response.status_code == 200:
                            result = response.json()
                            
                            if result.get('valid'):
                                # Add FK
                                foreign_keys.append({
                                    "column": fk_column,
                                    "ref_table": fk_ref_table,
                                    "ref_column": fk_ref_column
                                })
                                st.success(result.get('message'))
                                st.rerun()
                            else:
                                st.error(result.get('error'))
                                if result.get('available_columns'):
                                    st.info(f"Available columns: {', '.join(result['available_columns'])}")
                        else:
                            st.error(f"Validation failed: {response.text}")
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
        
        st.markdown("---")
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("← Back"):
                prev_step()
                st.rerun()
        with col2:
            if st.button("Skip Foreign Keys", type="secondary"):
                st.session_state.wizard_data['foreign_keys'] = []
                next_step()
                st.rerun()
        with col3:
            if st.button("Next: Confirm →", type="primary"):
                next_step()
                st.rerun()
    
    elif current_step == 6:
        st.subheader("Step 6: Review & Confirm")
        
        wizard_data = st.session_state.wizard_data
        schema = wizard_data.get('schema', {})
        
        st.markdown("#### Upload Summary")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**File:**")
            st.text(wizard_data.get('filename', 'N/A'))
            
            st.markdown("**Table Name:**")
            st.text(wizard_data.get('table_name', 'N/A'))
            
            st.markdown("**Rows:**")
            st.text(schema.get('row_count', 0))
        
        with col2:
            st.markdown("**Primary Key:**")
            pk = wizard_data.get('primary_key', '')
            st.text(pk if pk else "(None)")
            
            st.markdown("**Foreign Keys:**")
            fks = wizard_data.get('foreign_keys', [])
            if fks:
                for fk in fks:
                    st.text(f"{fk['column']} → {fk['ref_table']}.{fk['ref_column']}")
            else:
                st.text("(None)")
        
        st.markdown("---")
        st.markdown("#### Generated SQL")
        
        try:
            uploaded_file = wizard_data['file']
            uploaded_file.seek(0)
            
            header_row = wizard_data.get('header_row', 0)
            skip_rows_str = ",".join(map(str, range(header_row))) if header_row > 0 else None
            
            import json
            files = {"file": (uploaded_file.name, uploaded_file.read(), "text/csv")}
            data = {
                "header_row": header_row,
                "skip_rows": skip_rows_str,
                "primary_key": wizard_data.get('primary_key', ''),
                "foreign_keys": json.dumps(wizard_data.get('foreign_keys', []))
            }
            
            response = requests.post(
                f"{BACKEND_URL}/api/upload/analyze",
                files=files,
                data=data,
                timeout=30
            )
            
            if response.status_code == 200:
                final_schema = response.json()
                st.code(final_schema.get('create_sql', ''), language='sql')
            else:
                st.error("Failed to generate SQL preview")
        
        except Exception as e:
            st.error(f"Error generating preview: {str(e)}")
        
        st.markdown("---")
        
        if_exists = st.selectbox(
            "If table exists:",
            ["fail", "replace", "append"],
            help="fail: Error if exists | replace: Drop and recreate | append: Add rows"
        )
        
        st.markdown("---")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("← Back"):
                prev_step()
                st.rerun()
        
        with col2:
            if st.button("Start Over", type="secondary"):
                reset_wizard()
                st.rerun()
        
        with col3:
            if st.button("Upload to Database", type="primary"):
                with st.spinner("Uploading..."):
                    try:
                        wizard_data = st.session_state.wizard_data
                        uploaded_file = wizard_data['file']
                        uploaded_file.seek(0)
                        
                        header_row = wizard_data.get('header_row', 0)
                        skip_rows_str = ",".join(map(str, range(header_row))) if header_row > 0 else None
                        
                        pk = wizard_data.get('primary_key')
                        
                        if pk == "__AUTO__":
                            pk_to_send = ""  
                        elif pk == "__NONE__":
                            pk_to_send = "NONE" 
                        elif pk is None:
                            pk_to_send = ""  
                        else:
                            pk_to_send = pk 
                        
                        import json
                        files = {"file": (uploaded_file.name, uploaded_file.read(), "text/csv")}
                        data = {
                            "table_name": wizard_data.get('table_name'),
                            "if_exists": if_exists,
                            "header_row": header_row,
                            "skip_rows": skip_rows_str,
                            "primary_key": pk_to_send,  
                            "foreign_keys": json.dumps(wizard_data.get('foreign_keys', []))
                        }
                        
                        response = requests.post(
                            f"{BACKEND_URL}/api/upload/",
                            files=files,
                            data=data,
                            timeout=120
                        )
                        
                        if response.status_code == 200:
                            result = response.json()
                            
                            st.balloons()
                            st.success(f"✓ Successfully uploaded {result['rows_inserted']} rows to '{result['table_name']}'!")
                            
                            
                            reset_wizard()
                            
                            st.info("You can now query this data in the 'Query Data' page")
                            
                            if st.button("Upload Another File"):
                                st.rerun()
                        else:
                            st.error(f"Upload failed: {response.text}")
                            
                    except Exception as e:
                        st.error(f"Error: {str(e)}")


elif page == "Query Data":
    st.header("Natural Language Query")
    
    st.markdown("Ask questions about your data in plain English!")
    
    selected_table = None
    try:
        tables_response = requests.get(f"{BACKEND_URL}/api/database/tables", timeout=5)
        if tables_response.status_code == 200:
            tables_data = tables_response.json()
            tables = tables_data.get('tables', [])
            
            if tables:
                col1, col2 = st.columns([3, 1])
                with col1:
                    selected_table = st.selectbox(
                        "Select table to query:",
                        ["All tables"] + tables,
                        help="Choose a specific table or query across all tables"
                    )
                with col2:
                    st.metric("Available Tables", len(tables))
                
                
                if selected_table and selected_table != "All tables":
                    count_response = requests.get(
                        f"{BACKEND_URL}/api/database/tables/{selected_table}/count",
                        timeout=5
                    )
                    if count_response.status_code == 200:
                        count_data = count_response.json()
                        st.info(f"Selected: **{selected_table}** ({count_data.get('row_count', 0)} rows)")
            else:
                st.warning("No tables found. Please upload CSV files first.")
                selected_table = None
    except Exception as e:
        st.error(f"Error loading tables: {str(e)}")
        selected_table = None
    
    
    try:
        suggestions_response = requests.get(f"{BACKEND_URL}/api/query/suggestions", timeout=5)
        if suggestions_response.status_code == 200:
            suggestions_data = suggestions_response.json()
            suggestions = suggestions_data.get('suggestions', [])
            
            stats = suggestions_data.get('database_stats', {})
            if stats.get('tables'):
                total_rows = sum(t['row_count'] for t in stats['tables'])
                st.caption(f"Database: {stats['table_count']} tables | {total_rows} total rows")
    except:
        suggestions = []
    
    st.markdown("---")
    
    
    question = st.text_area(
        "Enter your question:",
        placeholder="e.g., How many records are there? What is the average value?",
        height=100,
        help="Ask questions about your data in plain English"
    )
    
    
    if suggestions:
        st.caption("Suggested questions:")
        cols = st.columns(3)
        for i, suggestion in enumerate(suggestions[:6]):
            with cols[i % 3]:
                if st.button(suggestion, key=f"suggest_{i}"):
                    question = suggestion
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        execute_query = st.checkbox("Execute query", value=True, help="Uncheck to only generate SQL without executing")
    
    with col2:
        result_limit = st.number_input("Result limit", min_value=10, max_value=1000, value=100, step=10)
    
    if st.button("Search", type="primary", disabled=not question):
        if question:
            with st.spinner("Generating SQL and fetching results..."):
                try:
                    request_data = {
                        "question": question,
                        "execute": execute_query,
                        "limit": result_limit
                    }
                    
                    if selected_table and selected_table != "All tables":
                        request_data["table_name"] = selected_table
                    
                    response = requests.post(
                        f"{BACKEND_URL}/api/query/",
                        json=request_data,
                        timeout=60
                    )
                    
                    if response.status_code == 200:
                        result = response.json()
                        
                        st.markdown('<div class="success-box">', unsafe_allow_html=True)
                        st.success("Query successful!")
                        st.markdown('</div>', unsafe_allow_html=True)
                        st.subheader("Generated SQL")
                        st.code(result['sql'], language='sql')
                        
                        st.caption(f"Model: {result.get('model', 'Unknown')}")
                        if result.get('executed'):
                            st.subheader("Results")
                            
                            col1, col2 = st.columns(2)
                            with col1:
                                st.metric("Rows Returned", result['row_count'])
                            with col2:
                                st.metric("Columns", len(result.get('columns', [])))
                            
                            if result.get('data'):
                                df = pd.DataFrame(result['data'])
                                st.dataframe(df, use_container_width=True)
                                
                                csv = df.to_csv(index=False)
                                st.download_button(
                                    label="Download as CSV",
                                    data=csv,
                                    file_name=f"query_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                    mime="text/csv"
                                )
                                
                                numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns.tolist()
                                if len(numeric_cols) > 0 and len(df) > 1:
                                    st.subheader("Visualization")
                                    
                                    if len(df.columns) == 2 and len(numeric_cols) == 1:
                                        st.bar_chart(df.set_index(df.columns[0]))
                                    elif len(numeric_cols) > 0:
                                        st.line_chart(df[numeric_cols])
                            else:
                                st.info("No data returned")
                        
                        with st.expander("View Raw Response"):
                            st.json(result)
                    
                    else:
                        st.markdown('<div class="error-box">', unsafe_allow_html=True)
                        st.error(f"Query failed: {response.status_code}")
                        st.write(response.text)
                        st.markdown('</div>', unsafe_allow_html=True)
                        
                except Exception as e:
                    st.markdown('<div class="error-box">', unsafe_allow_html=True)
                    st.error(f"Error: {str(e)}")
                    st.markdown('</div>', unsafe_allow_html=True)


elif page == "View Tables":
    st.header("Database Tables")
    
    try:
        response = requests.get(f"{BACKEND_URL}/api/database/tables", timeout=5)
        if response.status_code == 200:
            tables_data = response.json()
            tables = tables_data.get('tables', [])
            
            if tables:
                st.success(f"Found {len(tables)} table(s)")
                
                
                selected_table = st.selectbox("Select a table to view:", tables)
                
                if selected_table:
                    
                    table_response = requests.get(
                        f"{BACKEND_URL}/api/database/tables/{selected_table}",
                        timeout=5
                    )
                    
                    if table_response.status_code == 200:
                        table_info = table_response.json()
                        
                        
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            count_response = requests.get(
                                f"{BACKEND_URL}/api/database/tables/{selected_table}/count",
                                timeout=5
                            )
                            if count_response.status_code == 200:
                                count_data = count_response.json()
                                st.metric("Total Rows", count_data.get('row_count', 0))
                        
                        with col2:
                            st.metric("Columns", len(table_info.get('columns', [])))
                        
                        with col3:
                            st.metric("Primary Keys", len(table_info.get('primary_keys', [])))
                        
                        
                        with st.expander("Column Details"):
                            if table_info.get('columns'):
                                cols_df = pd.DataFrame(table_info['columns'])
                                st.dataframe(cols_df, use_container_width=True)
                        
                        
                        st.subheader("Sample Data")
                        sample_limit = st.slider("Number of rows to display:", 5, 100, 10)
                        
                        sample_response = requests.get(
                            f"{BACKEND_URL}/api/database/tables/{selected_table}/sample?limit={sample_limit}",
                            timeout=10
                        )
                        
                        if sample_response.status_code == 200:
                            sample_data = sample_response.json()
                            if sample_data.get('data'):
                                df = pd.DataFrame(sample_data['data'])
                                st.dataframe(df, use_container_width=True)
                            else:
                                st.info("No data in table")
            else:
                st.info("No tables found. Upload some CSV files to get started!")
                
        else:
            st.error("Failed to fetch tables")
            
    except Exception as e:
        st.error(f"Error: {str(e)}")

elif page == "Analytics":
    st.header("Analytics Dashboard")
    
    st.info("Access the Metabase dashboard for advanced analytics and visualizations")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Metabase Dashboard")
        st.markdown("""
        Metabase provides:
        - Interactive dashboards
        - Custom visualizations
        - Scheduled reports
        - Data exploration tools
        """)
        
        if st.button("Open Metabase", type="primary"):
            st.markdown('[Open Metabase in new tab](http://localhost:3000)', unsafe_allow_html=True)
    
    with col2:
        st.subheader("Quick Stats")
        
        try:
            response = requests.get(f"{BACKEND_URL}/api/query/suggestions", timeout=5)
            if response.status_code == 200:
                data = response.json()
                stats = data.get('database_stats', {})
                
                if stats.get('tables'):
                    for table in stats['tables']:
                        st.metric(table['name'], f"{table['row_count']:,} rows")
        except:
            pass
    
    st.markdown("---")
    st.caption("Metabase is available at: http://localhost:3000")
