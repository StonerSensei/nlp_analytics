import streamlit as st
import requests
import pandas as pd
import os
import json
from datetime import datetime

# Configuration
BACKEND_URL = os.getenv("BACKEND_API_URL", "http://backend:8000")

# Page configuration
st.set_page_config(
    page_title="Hospital Analytics",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
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

# Title
st.markdown('<p class="main-header">🏥 Hospital Analytics Platform</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Natural Language to SQL Query System</p>', unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.header("Navigation")
    page = st.radio(
        "Go to:",
        ["🏠 Home", "📁 Upload CSV", "🔍 Query Data", "📊 View Tables", "📈 Analytics"],
        key="navigation"
    )
    
    st.markdown("---")
    
    # System Status
    st.subheader("System Status")
    try:
        response = requests.get(f"{BACKEND_URL}/health", timeout=5)
        if response.status_code == 200:
            health = response.json()
            st.success("✅ Backend Online")
            
            if health.get("database") == "connected":
                st.success("✅ Database Connected")
            else:
                st.error("❌ Database Disconnected")
            
            if health.get("ollama") == "connected":
                st.success("✅ AI Model Ready")
            else:
                st.warning("⚠️ AI Model Not Ready")
        else:
            st.error("❌ Backend Error")
    except Exception as e:
        st.error("❌ Cannot reach backend")
        st.caption(f"Error: {str(e)}")
    
    st.markdown("---")
    st.caption("Powered by Ollama & SQLCoder")

# Home Page
if page == "🏠 Home":
    st.header("Welcome to Hospital Analytics")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.info("**📁 Upload Data**\n\nUpload CSV files to automatically create database tables")
    
    with col2:
        st.info("**🔍 Query**\n\nAsk questions in natural language and get instant answers")
    
    with col3:
        st.info("**📊 Analyze**\n\nVisualize your data with auto-generated charts")
    
    st.markdown("---")
    
    # Quick Start Guide
    st.subheader("Quick Start Guide")
    
    st.markdown("""
    ### 1️⃣ Upload Your Data
    - Go to **Upload CSV** page
    - Select your CSV file
    - Table will be automatically created
    
    ### 2️⃣ Ask Questions
    - Go to **Query Data** page
    - Type your question in plain English
    - Get SQL query and results instantly
    
    ### 3️⃣ Explore Tables
    - Go to **View Tables** page
    - Browse all your tables
    - View sample data and statistics
    
    ### 4️⃣ Analyze
    - Go to **Analytics** page
    - Access Metabase dashboard
    - Create custom visualizations
    """)
    
    st.markdown("---")
    
    # Example Questions
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

# Upload CSV Page
elif page == "📁 Upload CSV":
    st.header("📁 Upload CSV File")
    
    st.markdown("""
    Upload a CSV file to automatically:
    - Detect schema and data types
    - Identify primary and foreign keys
    - Create database table
    - Load data
    """)
    
    # File uploader
    uploaded_file = st.file_uploader("Choose a CSV file", type=['csv'])
    
    col1, col2 = st.columns(2)
    
    with col1:
        custom_table_name = st.text_input("Custom table name (optional)", placeholder="Leave empty for auto-generation")
    
    with col2:
        if_exists = st.selectbox(
            "If table exists:",
            ["fail", "replace", "append"],
            help="fail: Error if exists | replace: Drop and recreate | append: Add data to existing"
        )
    
    if uploaded_file is not None:
        st.success(f"✅ File selected: **{uploaded_file.name}**")
        st.caption(f"Size: {uploaded_file.size:,} bytes")
        
        # Preview CSV
        if st.checkbox("Preview CSV content"):
            try:
                preview_df = pd.read_csv(uploaded_file)
                st.dataframe(preview_df.head(10))
                uploaded_file.seek(0)  # Reset file pointer
            except Exception as e:
                st.error(f"Error reading CSV: {e}")
        
        # Upload button
        if st.button("🚀 Upload and Process", type="primary"):
            with st.spinner("Processing CSV file..."):
                try:
                    # Prepare request
                    files = {"file": (uploaded_file.name, uploaded_file.getvalue(), "text/csv")}
                    data = {
                        "table_name": custom_table_name if custom_table_name else "",
                        "if_exists": if_exists
                    }
                    
                    # Upload
                    response = requests.post(
                        f"{BACKEND_URL}/api/upload/",
                        files=files,
                        data=data,
                        timeout=60
                    )
                    
                    if response.status_code == 200:
                        result = response.json()
                        
                        st.markdown('<div class="success-box">', unsafe_allow_html=True)
                        st.success("✅ CSV uploaded and processed successfully!")
                        st.markdown('</div>', unsafe_allow_html=True)
                        
                        # Display results
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Table Name", result['table_name'])
                        with col2:
                            st.metric("Rows Inserted", result['rows_inserted'])
                        with col3:
                            st.metric("Columns", len(result['schema']['columns']))
                        
                        # Schema details
                        with st.expander("📋 View Detected Schema"):
                            st.json(result['schema'])
                        
                        # SQL
                        with st.expander("📝 View Generated SQL"):
                            st.code(result['schema']['create_sql'], language='sql')
                        
                        # Sample data
                        if result['schema'].get('sample_data'):
                            with st.expander("👀 View Sample Data"):
                                st.dataframe(pd.DataFrame(result['schema']['sample_data']))
                        
                    else:
                        st.markdown('<div class="error-box">', unsafe_allow_html=True)
                        st.error(f"❌ Upload failed: {response.status_code}")
                        st.write(response.text)
                        st.markdown('</div>', unsafe_allow_html=True)
                        
                except Exception as e:
                    st.markdown('<div class="error-box">', unsafe_allow_html=True)
                    st.error(f"❌ Error: {str(e)}")
                    st.markdown('</div>', unsafe_allow_html=True)

# Query Data Page
elif page == "🔍 Query Data":
    st.header("🔍 Natural Language Query")
    
    st.markdown("Ask questions about your data in plain English!")
    
    # Get suggestions
    try:
        suggestions_response = requests.get(f"{BACKEND_URL}/api/query/suggestions", timeout=5)
        if suggestions_response.status_code == 200:
            suggestions_data = suggestions_response.json()
            suggestions = suggestions_data.get('suggestions', [])
            
            # Display database stats
            stats = suggestions_data.get('database_stats', {})
            if stats.get('tables'):
                st.info(f"📊 **Database**: {stats['table_count']} tables | Total rows: {sum(t['row_count'] for t in stats['tables'])}")
    except:
        suggestions = []
    
    # Query input
    question = st.text_area(
        "Enter your question:",
        placeholder="e.g., How many employees are in each department?",
        height=100
    )
    
    # Suggestions
    if suggestions:
        st.caption("💡 Suggested questions:")
        cols = st.columns(3)
        for i, suggestion in enumerate(suggestions[:6]):
            with cols[i % 3]:
                if st.button(suggestion, key=f"suggest_{i}"):
                    question = suggestion
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        execute_query = st.checkbox("Execute query", value=True)
    
    with col2:
        result_limit = st.number_input("Result limit", min_value=10, max_value=1000, value=100, step=10)
    
    # Query button
    if st.button("🔍 Search", type="primary", disabled=not question):
        if question:
            with st.spinner("🤖 Generating SQL and fetching results..."):
                try:
                    # Send query
                    response = requests.post(
                        f"{BACKEND_URL}/api/query/",
                        json={
                            "question": question,
                            "execute": execute_query,
                            "limit": result_limit
                        },
                        timeout=60
                    )
                    
                    if response.status_code == 200:
                        result = response.json()
                        
                        st.markdown('<div class="success-box">', unsafe_allow_html=True)
                        st.success("✅ Query successful!")
                        st.markdown('</div>', unsafe_allow_html=True)
                        
                        # Display SQL
                        st.subheader("Generated SQL")
                        st.code(result['sql'], language='sql')
                        
                        # Display results if executed
                        if result.get('executed'):
                            st.subheader("Results")
                            
                            col1, col2 = st.columns(2)
                            with col1:
                                st.metric("Rows Returned", result['row_count'])
                            with col2:
                                st.metric("Columns", len(result.get('columns', [])))
                            
                            if result.get('data'):
                                # Display as dataframe
                                df = pd.DataFrame(result['data'])
                                st.dataframe(df, use_container_width=True)
                                
                                # Download button
                                csv = df.to_csv(index=False)
                                st.download_button(
                                    label="📥 Download as CSV",
                                    data=csv,
                                    file_name=f"query_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                    mime="text/csv"
                                )
                                
                                # Simple visualization for numeric data
                                numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns.tolist()
                                if len(numeric_cols) > 0 and len(df) > 1:
                                    st.subheader("📊 Visualization")
                                    
                                    if len(df.columns) == 2 and len(numeric_cols) == 1:
                                        # Bar chart
                                        st.bar_chart(df.set_index(df.columns[0]))
                                    elif len(numeric_cols) > 0:
                                        # Line chart
                                        st.line_chart(df[numeric_cols])
                            else:
                                st.info("No data returned")
                        
                        # Show raw response
                        with st.expander("🔍 View Raw Response"):
                            st.json(result)
                    
                    else:
                        st.markdown('<div class="error-box">', unsafe_allow_html=True)
                        st.error(f"❌ Query failed: {response.status_code}")
                        st.write(response.text)
                        st.markdown('</div>', unsafe_allow_html=True)
                        
                except Exception as e:
                    st.markdown('<div class="error-box">', unsafe_allow_html=True)
                    st.error(f"❌ Error: {str(e)}")
                    st.markdown('</div>', unsafe_allow_html=True)

# View Tables Page
elif page == "📊 View Tables":
    st.header("📊 Database Tables")
    
    # Get all tables
    try:
        response = requests.get(f"{BACKEND_URL}/api/database/tables", timeout=5)
        if response.status_code == 200:
            tables_data = response.json()
            tables = tables_data.get('tables', [])
            
            if tables:
                st.success(f"Found {len(tables)} table(s)")
                
                # Table selector
                selected_table = st.selectbox("Select a table to view:", tables)
                
                if selected_table:
                    # Get table info
                    table_response = requests.get(
                        f"{BACKEND_URL}/api/database/tables/{selected_table}",
                        timeout=5
                    )
                    
                    if table_response.status_code == 200:
                        table_info = table_response.json()
                        
                        # Table statistics
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
                        
                        # Column details
                        with st.expander("📋 Column Details"):
                            if table_info.get('columns'):
                                cols_df = pd.DataFrame(table_info['columns'])
                                st.dataframe(cols_df, use_container_width=True)
                        
                        # Sample data
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
                st.info("📭 No tables found. Upload some CSV files to get started!")
                
        else:
            st.error("Failed to fetch tables")
            
    except Exception as e:
        st.error(f"Error: {str(e)}")

# Analytics Page
elif page == "📈 Analytics":
    st.header("📈 Analytics Dashboard")
    
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
        
        if st.button("🚀 Open Metabase", type="primary"):
            st.markdown('[Open Metabase in new tab](http://localhost:3000)', unsafe_allow_html=True)
    
    with col2:
        st.subheader("Quick Stats")
        
        try:
            # Get database stats
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
