import streamlit as st
import requests
import os

BACKEND_URL = os.getenv("BACKEND_API_URL", "http://backend:8000")

st.set_page_config(
    page_title="Hospital Analytics",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("Hospital Analytics Platform")
st.markdown("### Natural Language to SQL Query System")

with st.sidebar:
    st.header("Navigation")
    page = st.radio(
        "Go to:",
        ["Home", "Upload CSV", "Query Data", "Analytics"]
    )

if page == "Home":
    st.subheader("Welcome to Hospital Analytics")
    st.write("This platform allows you to:")
    st.write("- Upload CSV files")
    st.write("- Query data using natural language")
    st.write("- View analytics and dashboards")
    
    try:
        response = requests.get(f"{BACKEND_URL}/health", timeout=5)
        if response.status_code == 200:
            st.success("Connected to backend")
        else:
            st.error("Backend connection failed")
    except Exception as e:
        st.error(f"Cannot reach backend: {e}")

elif page == "Upload CSV":
    st.subheader("Upload CSV File")
    uploaded_file = st.file_uploader("Choose a CSV file", type=['csv'])
    
    if uploaded_file is not None:
        st.write(f"**File name:** {uploaded_file.name}")
        st.write(f"**File size:** {uploaded_file.size} bytes")
        
        if st.button("Upload and Process"):
            with st.spinner("Processing..."):
                st.info("Upload functionality will be implemented in next section")

elif page == "Query Data":
    st.subheader("🔍 Query Your Data")
    question = st.text_input("Enter your question:", 
                             placeholder="e.g., How many employees have more than 3 years experience?")
    
    if st.button("Search"):
        if question:
            with st.spinner("Generating SQL and fetching results..."):
                st.info("Query functionality will be implemented in next section")
        else:
            st.warning("Please enter a question")

elif page == "Analytics":
    st.subheader("Analytics Dashboard")
    st.info("Analytics will be powered by Metabase")
    st.write(f"Access Metabase at: [http://localhost:3000](http://localhost:3000)")
