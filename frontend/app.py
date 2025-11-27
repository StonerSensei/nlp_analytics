import streamlit as st
import requests
import pandas as pd
import os
import plotly.express as px
import plotly.graph_objects as go

# Backend API URL
BACKEND_URL = os.getenv("BACKEND_API_URL", "http://backend:8000")

st.set_page_config(page_title="Hospital Data Analysis", layout="wide")

st.title("🏥 Hospital Data Analysis - NLP to SQL")

# Sidebar
with st.sidebar:
    st.header("Navigation")
    page = st.radio("Select Page", ["Upload CSV", "Analytics Dashboard","Query Data", "View Tables", "Execute SQL"])
    
    st.markdown("---")
    st.caption(f"Backend: {BACKEND_URL}")

# Upload CSV Page
if page == "Upload CSV":
    st.header("Upload CSV Files")
    
    st.info("Upload HIS.csv (skip 0 rows) and RIS.csv (skip 5 rows)")
    
    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
    
    col1, col2 = st.columns(2)
    with col1:
        table_name = st.text_input("Table Name", value="his" if uploaded_file and "HIS" in uploaded_file.name.upper() else "ris")
    with col2:
        skip_rows = st.number_input("Skip Rows (for headers)", min_value=0, value=5 if uploaded_file and "RIS" in uploaded_file.name.upper() else 0)
    
    if st.button("Upload and Process", type="primary"):
        if uploaded_file and table_name:
            with st.spinner("Processing..."):
                try:
                    files = {"file": (uploaded_file.name, uploaded_file.getvalue(), "text/csv")}
                    data = {"table_name": table_name, "skip_rows": skip_rows}
                    
                    response = requests.post(f"{BACKEND_URL}/upload-csv", files=files, data=data)
                    
                    if response.status_code == 200:
                        result = response.json()
                        st.success(f"{result['message']}")
                        st.info(f"Rows: {result['rows']} | Columns: {len(result['columns'])}")
                        
                        with st.expander("View Columns"):
                            st.write(result['columns'])
                    else:
                        st.error(f"Error: {response.json().get('detail', 'Unknown error')}")
                except Exception as e:
                    st.error(f"Connection error: {str(e)}")
        else:
            st.warning("Please upload a file and provide a table name")

# Query Data Page
elif page == "Query Data":
    st.header("Natural Language Query")
    
    with st.expander("Example Queries"):
        st.markdown("""
        - Show me all patients from HIS table
        - Find patients with multiple services
        - Count how many times each bill_id appears in HIS
        - Find bill_ids from HIS that don't exist as patient_id in RIS
        - Show patient names who have more than 2 services
        """)
    
    query = st.text_area("Enter your question:", height=100, placeholder="e.g., Show all patients with bill_id 576780357")
    
    if st.button("Run Query", type="primary"):
        if query:
            with st.spinner("Processing query..."):
                try:
                    response = requests.post(f"{BACKEND_URL}/query", data={"query": query})
                    
                    if response.status_code == 200:
                        result = response.json()
                        
                        st.subheader("Generated SQL:")
                        st.code(result['sql_query'], language='sql')
                        
                        st.subheader(f"Results ({result['row_count']} rows):")
                        if result['results']:
                            df = pd.DataFrame(result['results'])
                            st.dataframe(df, use_container_width=True)
                            
                            # Download button
                            csv = df.to_csv(index=False)
                            st.download_button("Download CSV", csv, "query_results.csv", "text/csv")
                        else:
                            st.info("No results found")
                    else:
                        st.error(f"Error: {response.json().get('detail', 'Unknown error')}")
                except Exception as e:
                    st.error(f"Connection error: {str(e)}")
        else:
            st.warning("Please enter a query")

# View Tables Page
elif page == "View Tables":
    st.header("Database Tables")
    
    # Auto-load tables on page load
    try:
        response = requests.get(f"{BACKEND_URL}/tables")
        
        if response.status_code == 200:
            tables = response.json()['tables']
            
            if tables:
                st.success(f"Found {len(tables)} table(s)")
                
                for table in tables:
                    with st.expander(f" {table['name'].upper()} ({table['row_count']} rows)"):
                        st.write("**Columns:**", ", ".join(table['columns']))
                        
                        # View data button
                        if st.button("View Data", key=f"view_{table['name']}"):
                            with st.spinner("Loading data..."):
                                limit = st.session_state.get(f"limit_{table['name']}", 50)
                                data_response = requests.get(f"{BACKEND_URL}/table/{table['name']}", params={"limit": limit})
                                
                                if data_response.status_code == 200:
                                    data = data_response.json()['data']
                                    if data:
                                        df = pd.DataFrame(data)
                                        st.dataframe(df, use_container_width=True)
                                        
                                        # Download button
                                        csv = df.to_csv(index=False)
                                        st.download_button(
                                            "Download CSV",
                                            csv,
                                            f"{table['name']}_data.csv",
                                            "text/csv",
                                            key=f"download_{table['name']}"
                                        )
                                    else:
                                        st.info("No data in table")
                                else:
                                    st.error(f"Failed to load data: {data_response.status_code}")
                        
                        # Limit selector
                        limit = st.number_input(
                            "Rows to display",
                            min_value=10,
                            max_value=1000,
                            value=50,
                            step=10,
                            key=f"limit_{table['name']}"
                        )
                        
                        # Delete button
                        if st.button("Delete Table", key=f"delete_{table['name']}"):
                            st.warning(f"Are you sure you want to delete {table['name']}?")
                            
                            col1, col2 = st.columns(2)
                            with col1:
                                if st.button("Yes, Delete", key=f"confirm_{table['name']}", type="primary"):
                                    delete_response = requests.delete(f"{BACKEND_URL}/table/{table['name']}")
                                    if delete_response.status_code == 200:
                                        st.success(f"Deleted {table['name']}")
                                        st.rerun()
                                    else:
                                        st.error("Failed to delete table")
                            with col2:
                                if st.button("Cancel", key=f"cancel_{table['name']}"):
                                    st.info("Delete cancelled")
            else:
                st.info("No tables found. Upload CSV files to get started.")
                st.markdown("Go to **Upload CSV** page to upload HIS.csv and RIS.csv files.")
        else:
            st.error(f"Failed to connect to backend. Status code: {response.status_code}")
            st.info(f"Backend URL: {BACKEND_URL}")
            st.info("Make sure the backend service is running.")
    
    except requests.exceptions.ConnectionError:
        st.error("Cannot connect to backend API")
        st.info(f"Backend URL: {BACKEND_URL}")
        st.info("Please check if Docker containers are running: `docker-compose ps`")
    except Exception as e:
        st.error(f"Error: {str(e)}")
    
    # Manual refresh button
    if st.button("Refresh", key="manual_refresh"):
        st.rerun()

# Execute SQL Page
elif page == "Execute SQL":
    st.header("Execute SQL Query")
    
    with st.expander("Example SQL Queries"):
        st.code("""
-- Get all records from HIS
SELECT * FROM his LIMIT 10;

-- Count bill_id occurrences
SELECT bill_id, COUNT(*) as count 
FROM his 
GROUP BY bill_id 
ORDER BY count DESC;

-- Find missing records
SELECT h.bill_id 
FROM his h 
LEFT JOIN ris r ON h.bill_id = r.patient_id 
WHERE r.patient_id IS NULL;
        """, language='sql')
    
    sql_query = st.text_area("Enter SQL Query:", height=200, placeholder="SELECT * FROM his LIMIT 10;")
    
    if st.button("Execute", type="primary"):
        if sql_query:
            with st.spinner("Executing..."):
                try:
                    response = requests.post(f"{BACKEND_URL}/execute-sql", data={"sql": sql_query})
                    
                    if response.status_code == 200:
                        result = response.json()
                        
                        if 'results' in result:
                            st.success(f"Query executed successfully ({result['row_count']} rows)")
                            if result['results']:
                                df = pd.DataFrame(result['results'])
                                st.dataframe(df, use_container_width=True)
                                
                                csv = df.to_csv(index=False)
                                st.download_button("Download CSV", csv, "sql_results.csv", "text/csv")
                            else:
                                st.info("Query returned no results")
                        else:
                            st.success(result['message'])
                    else:
                        st.error(f"Error: {response.json().get('detail', 'Unknown error')}")
                except Exception as e:
                    st.error(f"Connection error: {str(e)}")
        else:
            st.warning("Please enter a SQL query")


# Data Validation Page
elif page == "Data Validation":
    st.header("Data Validation - HIS vs RIS")
    
    st.info("Compare patient records and services between HIS (Government) and RIS (Private) files")
    
    if st.button("Run Validation", type="primary"):
        with st.spinner("Validating data..."):
            try:
                response = requests.get(f"{BACKEND_URL}/validate-data")
                
                if response.status_code == 200:
                    data = response.json()
                    summary = data['summary']
                    
                    # Summary Metrics
                    st.subheader("Summary")
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric("HIS Records", summary['his_total_records'])
                        st.metric("Unique Bill IDs", summary['his_unique_bill_ids'])
                    
                    with col2:
                        st.metric("RIS Records", summary['ris_total_records'])
                        st.metric("Unique Patient IDs", summary['ris_unique_patient_ids'])
                    
                    with col3:
                        st.metric("Missing in RIS", summary['missing_in_ris_count'], 
                                 delta=None if summary['missing_in_ris_count'] == 0 else "Issues",
                                 delta_color="inverse")
                    
                    with col4:
                        st.metric("Mismatched Services", summary['mismatched_count'],
                                 delta=None if summary['mismatched_count'] == 0 else "Issues",
                                 delta_color="inverse")
                    
                    # Missing Records
                    if summary['missing_in_ris_count'] > 0:
                        st.subheader("Bill IDs in HIS but NOT in RIS")
                        st.error(f"Found {summary['missing_in_ris_count']} bill_ids missing in RIS file")
                        missing_df = pd.DataFrame({"Bill ID": data['missing_in_ris']})
                        st.dataframe(missing_df, use_container_width=True)
                        
                        csv = missing_df.to_csv(index=False)
                        st.download_button("Download Missing IDs", csv, "missing_in_ris.csv", "text/csv")
                    
                    if summary['missing_in_his_count'] > 0:
                        st.subheader("Patient IDs in RIS but NOT in HIS")
                        st.warning(f"Found {summary['missing_in_his_count']} patient_ids missing in HIS file")
                        missing_df = pd.DataFrame({"Patient ID": data['missing_in_his']})
                        st.dataframe(missing_df, use_container_width=True)
                    
                    # Service Count Comparison
                    st.subheader("Service Count Comparison")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write("**HIS - Services per Bill ID (Top 50)**")
                        his_df = pd.DataFrame(data['his_service_counts'])
                        st.dataframe(his_df, use_container_width=True)
                    
                    with col2:
                        st.write("**RIS - Entries per Patient ID (Top 50)**")
                        ris_df = pd.DataFrame(data['ris_entry_counts'])
                        st.dataframe(ris_df, use_container_width=True)
                    
                    # Mismatched Records
                    if summary['mismatched_count'] > 0:
                        st.subheader("Mismatched Service Counts")
                        st.error(f"Found {summary['mismatched_count']} patients with different service counts")
                        mismatch_df = pd.DataFrame(data['mismatched_records'])
                        st.dataframe(mismatch_df, use_container_width=True)
                        
                        csv = mismatch_df.to_csv(index=False)
                        st.download_button("Download Mismatches", csv, "mismatched_services.csv", "text/csv")
                    else:
                        st.success("All patients have matching service counts between HIS and RIS!")
                    
                else:
                    st.error(f"Validation failed: {response.json().get('detail', 'Unknown error')}")
            
            except Exception as e:
                st.error(f"Error: {str(e)}")

# Analytics Dashboard Page
elif page == "Analytics Dashboard":
    st.header("Analytics Dashboard - HIS vs RIS Validation")
    
    st.info("Click on any analysis button to view results and visualizations")
    
    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9, tab10 = st.tabs([
        "Records Comparison",
        "Services per Patient", 
        "Missing in RIS",
        "Service Mismatch",
        "Daily Trends",
        "Top Services",
        "Service Comparison",
        "Order Type & Modality",
        "Worklist Details",
        "Comprehensive View"
    ])
    
    # TAB 1: Records Comparison
    with tab1:
        st.subheader("Total Records Comparison - HIS vs RIS")
        
        if st.button("Load Records Comparison", key="btn_records"):
            with st.spinner("Loading data..."):
                try:
                    response = requests.get(f"{BACKEND_URL}/analytics/records-comparison")
                    if response.status_code == 200:
                        data = response.json()['data']
                        df = pd.DataFrame(data)
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            his_data = df[df['source'] == 'HIS'].iloc[0]
                            st.metric("HIS Total Records", his_data['total_records'])
                            st.metric("HIS Unique Bill IDs", his_data['unique_ids'])
                        
                        with col2:
                            ris_data = df[df['source'] == 'RIS'].iloc[0]
                            st.metric("RIS Total Records", ris_data['total_records'])
                            st.metric("RIS Unique Patient IDs", ris_data['unique_ids'])
                        
                        st.markdown("### Visual Comparison")
                        
                        
                        fig = go.Figure(data=[
                            go.Bar(name='Total Records', x=df['source'], y=df['total_records']),
                            go.Bar(name='Unique IDs', x=df['source'], y=df['unique_ids'])
                        ])
                        fig.update_layout(
                            barmode='group',
                            title="HIS vs RIS Comparison",
                            xaxis_title="Source",
                            yaxis_title="Count",
                            height=400
                        )
                        st.plotly_chart(fig, use_container_width=True)
                        
                        st.markdown("### Data Table")
                        st.dataframe(df, use_container_width=True)
                        
                        csv = df.to_csv(index=False)
                        st.download_button("Download CSV", csv, "records_comparison.csv", "text/csv")
                    else:
                        st.error("Failed to load data")
                except Exception as e:
                    st.error(f"Error: {str(e)}")
    
    # TAB 2: Services per Patient
    with tab2:
        st.subheader("Services per Patient (Top 100)")
        
        if st.button("Load Services Analysis", key="btn_services"):
            with st.spinner("Loading data..."):
                try:
                    response = requests.get(f"{BACKEND_URL}/analytics/services-per-patient")
                    if response.status_code == 200:
                        data = response.json()['data']
                        df = pd.DataFrame(data)
                        
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total Patients", len(df))
                        with col2:
                            st.metric("Avg Services/Patient", f"{df['service_count'].mean():.2f}")
                        with col3:
                            st.metric("Max Services", df['service_count'].max())
                        
                        st.markdown("### Top 20 Patients by Service Count")
                        
                        
                        
                        top_20 = df.head(20)
                        fig = px.bar(
                            top_20,
                            x='patient_name',
                            y='service_count',
                            title='Top 20 Patients by Number of Services',
                            labels={'patient_name': 'Patient Name', 'service_count': 'Service Count'},
                            color='service_count',
                            color_continuous_scale='Blues'
                        )
                        fig.update_layout(xaxis_tickangle=-45, height=500)
                        st.plotly_chart(fig, use_container_width=True)
                        
                        st.markdown("### Complete Data Table")
                        st.dataframe(df, use_container_width=True)
                        
                        csv = df.to_csv(index=False)
                        st.download_button("Download CSV", csv, "services_per_patient.csv", "text/csv")
                    else:
                        st.error("Failed to load data")
                except Exception as e:
                    st.error(f"Error: {str(e)}")
    
    # TAB 3: Missing in RIS
    with tab3:
        st.subheader("Bill IDs in HIS but Missing in RIS")
        
        if st.button("Find Missing Records", key="btn_missing"):
            with st.spinner("Searching..."):
                try:
                    response = requests.get(f"{BACKEND_URL}/analytics/missing-in-ris")
                    if response.status_code == 200:
                        data = response.json()['data']
                        df = pd.DataFrame(data)
                        
                        if len(df) > 0:
                            st.error(f"Found {len(df)} bill IDs missing in RIS!")
                            
                            col1, col2 = st.columns(2)
                            with col1:
                                st.metric("Missing Bill IDs", len(df))
                            with col2:
                                st.metric("Total Missing Services", df['his_services'].sum())
                            
                            st.markdown("### Missing Services Distribution")
                            
                            fig = px.histogram(
                                df,
                                x='his_services',
                                title='Distribution of Services for Missing Bill IDs',
                                labels={'his_services': 'Number of Services', 'count': 'Number of Patients'},
                                color_discrete_sequence=['red']
                            )
                            st.plotly_chart(fig, use_container_width=True)
                            
                            st.markdown("### Missing Records")
                            st.dataframe(df, use_container_width=True)
                            
                            csv = df.to_csv(index=False)
                            st.download_button("Download Missing Records", csv, "missing_in_ris.csv", "text/csv")
                        else:
                            st.success("No missing records! All HIS bill IDs exist in RIS.")
                    else:
                        st.error("Failed to load data")
                except Exception as e:
                    st.error(f"Error: {str(e)}")
    
    # TAB 4: Service Mismatch
    with tab4:
        st.subheader("Patients with Different Service Counts")
        
        if st.button("Find Mismatches", key="btn_mismatch"):
            with st.spinner("Comparing..."):
                try:
                    response = requests.get(f"{BACKEND_URL}/analytics/service-mismatch")
                    if response.status_code == 200:
                        data = response.json()['data']
                        df = pd.DataFrame(data)
                        
                        if len(df) > 0:
                            st.error(f"Found {len(df)} patients with mismatched service counts!")
                            
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("Mismatched Patients", len(df))
                            with col2:
                                st.metric("Avg Difference", f"{df['difference'].mean():.2f}")
                            with col3:
                                st.metric("Max Difference", df['difference'].max())
                            
                            st.markdown("### HIS vs RIS Service Count Comparison")
                            
                            
                            top_10 = df.head(10)
                            
                            fig = go.Figure(data=[
                                go.Bar(name='HIS Count', x=top_10['his_name'], y=top_10['his_count'], marker_color='blue'),
                                go.Bar(name='RIS Count', x=top_10['ris_name'], y=top_10['ris_count'], marker_color='red')
                            ])
                            fig.update_layout(
                                barmode='group',
                                title='Top 10 Mismatched Patients - Service Count Comparison',
                                xaxis_title='Patient Name',
                                yaxis_title='Service Count',
                                height=500
                            )
                            fig.update_xaxes(tickangle=-45)
                            st.plotly_chart(fig, use_container_width=True)
                            
                            st.markdown("### Mismatch Details")
                            st.dataframe(df, use_container_width=True)
                            
                            csv = df.to_csv(index=False)
                            st.download_button("Download Mismatches", csv, "service_mismatches.csv", "text/csv")
                        else:
                            st.success("All patients have matching service counts!")
                    else:
                        st.error("Failed to load data")
                except Exception as e:
                    st.error(f"Error: {str(e)}")
    
    # TAB 5: Daily Trends
    with tab5:
        st.subheader("Daily Service Trends")
        
        if st.button("Load Daily Trends", key="btn_trends"):
            with st.spinner("Loading trends..."):
                try:
                    response = requests.get(f"{BACKEND_URL}/analytics/daily-trends")
                    if response.status_code == 200:
                        data = response.json()['data']
                        df = pd.DataFrame(data)
                        
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total Days", len(df))
                        with col2:
                            st.metric("Avg Services/Day", f"{df['total_services'].mean():.0f}")
                        with col3:
                            st.metric("Peak Day Services", df['total_services'].max())
                        
                        st.markdown("### Daily Service Trends")
                        
                        fig = px.line(
                            df,
                            x='date',
                            y=['total_services', 'unique_patients', 'service_types'],
                            title='Daily Service Trends Over Time',
                            labels={'value': 'Count', 'date': 'Date', 'variable': 'Metric'},
                            markers=True
                        )
                        fig.update_layout(height=500)
                        st.plotly_chart(fig, use_container_width=True)
                        
                        st.markdown("### Daily Data")
                        st.dataframe(df, use_container_width=True)
                        
                        csv = df.to_csv(index=False)
                        st.download_button("Download Trends", csv, "daily_trends.csv", "text/csv")
                    else:
                        st.error("Failed to load data")
                except Exception as e:
                    st.error(f"Error: {str(e)}")
    
    # TAB 6: Top Services
    with tab6:
        st.subheader("Top 20 Most Used Services")
        
        if st.button("Load Top Services", key="btn_top"):
            with st.spinner("Loading services..."):
                try:
                    response = requests.get(f"{BACKEND_URL}/analytics/top-services")
                    if response.status_code == 200:
                        data = response.json()['data']
                        df = pd.DataFrame(data)
                        
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total Service Types", len(df))
                        with col2:
                            st.metric("Most Popular Service", df.iloc[0]['service_description'][:30] + "...")
                        with col3:
                            st.metric("Usage Count", df.iloc[0]['count'])
                        
                        st.markdown("### Top 20 Services by Usage")
                        
                        fig = px.bar(
                            df,
                            y='service_description',
                            x='count',
                            orientation='h',
                            title='Top 20 Most Requested Services',
                            labels={'service_description': 'Service', 'count': 'Number of Times Requested'},
                            color='count',
                            color_continuous_scale='Viridis'
                        )
                        fig.update_layout(height=600, yaxis={'categoryorder': 'total ascending'})
                        st.plotly_chart(fig, use_container_width=True)
                        
                        st.markdown("### Unique Patients Distribution (Top 10)")
                        
                        top_10 = df.head(10)
                        fig2 = px.pie(
                            top_10,
                            values='unique_patients',
                            names='service_description',
                            title='Distribution of Unique Patients Across Top 10 Services'
                        )
                        st.plotly_chart(fig2, use_container_width=True)
                        
                        st.markdown("### Service Details")
                        st.dataframe(df, use_container_width=True)
                        
                        csv = df.to_csv(index=False)
                        st.download_button("Download Services", csv, "top_services.csv", "text/csv")
                    else:
                        st.error("Failed to load data")
                except Exception as e:
                    st.error(f"Error: {str(e)}")

    with tab7:
        st.subheader("HIS vs RIS Service Comparison by Patient")
        st.markdown("Compare service names/descriptions between HIS and RIS files for each patient")
        
        col1, col2 = st.columns([3, 1])
        with col1:
            search_query = st.text_input(
                "Search Patient Name",
                placeholder="Enter patient name (e.g., Liladhar, Gopal, etc.)",
                key="patient_search"
            )
        with col2:
            search_btn = st.button("Search", type="primary", key="search_patient_btn")
            show_all = st.button("Show All (Top 100)", key="show_all_btn")
        
        if search_btn or show_all or search_query:
            with st.spinner("Loading patient data..."):
                try:
                    if show_all:
                        search_param = ""
                    else:
                        search_param = search_query if search_query else ""
                    
                    response = requests.get(
                        f"{BACKEND_URL}/analytics/service-comparison",
                        params={"patient_name": search_param}
                    )
                    
                    if response.status_code == 200:
                        data = response.json()['data']
                        
                        if data:
                            df = pd.DataFrame(data)
                            
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.metric("Total Patients", len(df))
                            with col2:
                                matching = df[df['his_service_count'] == df['ris_service_count']].shape[0]
                                st.metric("Matching Counts", matching)
                            with col3:
                                mismatched = df[df['his_service_count'] != df['ris_service_count']].shape[0]
                                st.metric("Mismatched Counts", mismatched, delta_color="inverse")
                            with col4:
                                missing = df[(df['his_services'].isna()) | (df['ris_services'].isna())].shape[0]
                                st.metric("Missing in One File", missing, delta_color="inverse")
                            
                            st.markdown("---")
                            
                            display_mode = st.radio(
                                "Display Mode:",
                                ["All Patients", "Mismatched Only", "Missing in One File"],
                                horizontal=True
                            )
                            
                            if display_mode == "Mismatched Only":
                                filtered_df = df[df['his_service_count'] != df['ris_service_count']]
                            elif display_mode == "Missing in One File":
                                filtered_df = df[(df['his_services'].isna()) | (df['ris_services'].isna())]
                            else:
                                filtered_df = df
                            
                            st.markdown(f"### Patient Service Comparison ({len(filtered_df)} records)")
                            
                            for idx, row in filtered_df.iterrows():
                                with st.expander(
                                    f"👤 {row['patient_name']} - ID: {row['id']} "
                                    f"(HIS: {row['his_service_count'] or 0} services | "
                                    f"RIS: {row['ris_service_count'] or 0} services)"
                                ):
                                    col1, col2 = st.columns(2)
                                    
                                    with col1:
                                        st.markdown("**HIS Services:**")
                                        if row['his_services']:
                                            services = row['his_services'].split(' | ')
                                            for i, service in enumerate(services, 1):
                                                st.markdown(f"{i}. {service}")
                                        else:
                                            st.warning("No services in HIS")
                                    
                                    with col2:
                                        st.markdown("**RIS Services:**")
                                        if row['ris_services']:
                                            services = row['ris_services'].split(' | ')
                                            for i, service in enumerate(services, 1):
                                                st.markdown(f"{i}. {service}")
                                        else:
                                            st.warning("No services in RIS")
                                    
                                    if row['his_service_count'] != row['ris_service_count']:
                                        st.error(f"Service count mismatch: HIS has {row['his_service_count']}, RIS has {row['ris_service_count']}")
                            
                            st.markdown("---")
                            if st.checkbox("Show Compact Table View"):
                                
                                display_df = filtered_df[[
                                    'patient_name', 'id', 
                                    'his_service_count', 'ris_service_count',
                                    'his_services', 'ris_services'
                                ]].copy()
                                
                                display_df['his_services'] = display_df['his_services'].apply(
                                    lambda x: (x[:100] + '...') if x and len(x) > 100 else x
                                )
                                display_df['ris_services'] = display_df['ris_services'].apply(
                                    lambda x: (x[:100] + '...') if x and len(x) > 100 else x
                                )
                                
                                st.dataframe(display_df, use_container_width=True)
                            
                            st.markdown("---")
                            csv = filtered_df.to_csv(index=False)
                            st.download_button(
                                "Download Comparison Data",
                                csv,
                                f"service_comparison_{search_param or 'all'}.csv",
                                "text/csv"
                            )
                        else:
                            st.info(f"No patients found matching '{search_query}'")
                            st.markdown("Try different search terms or click 'Show All'")
                    else:
                        st.error("Failed to load data")
                except Exception as e:
                    st.error(f"Error: {str(e)}")
        else:
            st.info("Use the search bar above to find specific patients or click 'Show All' to see top 100 patients")
            
            st.markdown("""
            ### How to Use:
            
            1. **Search by Patient Name**: Type part of the patient name and click Search
            2. **Show All**: Display top 100 patients
            3. **Compare Services**: See side-by-side comparison of HIS vs RIS service descriptions
            4. **Filter Results**: Choose to show all, mismatched only, or missing records
            5. **Download**: Export comparison data to CSV
            
            ### Why This is Useful:
            
            - **Identify spelling differences** between HIS and RIS service names
            - **Find missing services** in either system
            - **Validate data consistency** across both files
            - **Spot data entry errors** by comparing service descriptions
            """)
    with tab8:
        st.subheader("Order Type & Modality - RIS vs Scan Detail")
        
        if st.button("Load Comparison", key="btn_order_modality"):
            with st.spinner("Loading data..."):
                try:
                    response = requests.get(f"{BACKEND_URL}/analytics/order-type-modality-comparison")
                    if response.status_code == 200:
                        data = response.json()['data']
                        df = pd.DataFrame(data)
                        
                        if len(df) > 0:
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.metric("Total Patients", len(df))
                            with col2:
                                order_match = df[df['order_type_status'] == 'Match'].shape[0]
                                st.metric("Order Type Match", order_match)
                            with col3:
                                modality_match = df[df['modality_status'] == 'Match'].shape[0]
                                st.metric("Modality Match", modality_match)
                            with col4:
                                both_mismatch = df[
                                    (df['order_type_status'] == 'Mismatch') | 
                                    (df['modality_status'] == 'Mismatch')
                                ].shape[0]
                                st.metric("Mismatches", both_mismatch, delta_color="inverse")
                            
                            st.markdown("### Comparison Charts")
                            
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                
                                status_df = df['order_type_status'].value_counts().reset_index()
                                status_df.columns = ['Status', 'Count']
                                fig = px.pie(
                                    status_df,
                                    values='Count',
                                    names='Status',
                                    title='Order Type Match Status'
                                )
                                st.plotly_chart(fig, use_container_width=True)
                            
                            with col2:
                                modality_df = df['modality_status'].value_counts().reset_index()
                                modality_df.columns = ['Status', 'Count']
                                fig2 = px.pie(
                                    modality_df,
                                    values='Count',
                                    names='Status',
                                    title='Modality Match Status'
                                )
                                st.plotly_chart(fig2, use_container_width=True)
                            
                            st.markdown("### Filter Results")
                            filter_option = st.radio(
                                "Show:",
                                ["All Records", "Order Type Mismatch Only", "Modality Mismatch Only", "Any Mismatch"],
                                horizontal=True
                            )
                            
                            if filter_option == "Order Type Mismatch Only":
                                filtered_df = df[df['order_type_status'] == 'Mismatch']
                            elif filter_option == "Modality Mismatch Only":
                                filtered_df = df[df['modality_status'] == 'Mismatch']
                            elif filter_option == "Any Mismatch":
                                filtered_df = df[
                                    (df['order_type_status'] == 'Mismatch') | 
                                    (df['modality_status'] == 'Mismatch')
                                ]
                            else:
                                filtered_df = df
                            
                            st.markdown(f"### Detailed Comparison ({len(filtered_df)} records)")
                            st.dataframe(filtered_df, use_container_width=True)
                            
                            csv = filtered_df.to_csv(index=False)
                            st.download_button("Download Data", csv, "order_modality_comparison.csv", "text/csv")
                        else:
                            st.info("No data found")
                    else:
                        st.error("Failed to load data")
                except Exception as e:
                    st.error(f"Error: {str(e)}")
    
    with tab9:
        st.subheader("Worklist - Study Details & Assignments")
        
        search_patient = st.text_input(
            "Search Patient in Worklist",
            placeholder="Enter patient name",
            key="worklist_search"
        )
        
        col1, col2 = st.columns([1, 1])
        with col1:
            search_btn = st.button("Search", type="primary", key="btn_worklist_search")
        with col2:
            show_all_btn = st.button("Show All (Top 100)", key="btn_worklist_all")
        
        if search_btn or show_all_btn:
            with st.spinner("Loading worklist..."):
                try:
                    search_param = "" if show_all_btn else search_patient
                    response = requests.get(
                        f"{BACKEND_URL}/analytics/worklist-details",
                        params={"patient_name": search_param}
                    )
                    
                    if response.status_code == 200:
                        data = response.json()['data']
                        df = pd.DataFrame(data)
                        
                        if len(df) > 0:
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.metric("Total Studies", len(df))
                            with col2:
                                assigned = df[df['assigned_to'].notna()].shape[0]
                                st.metric("Assigned Studies", assigned)
                            with col3:
                                finalized = df[df['report_finalized_by'].notna()].shape[0]
                                st.metric("Finalized Reports", finalized)
                            with col4:
                                unique_institutions = df['institution_name'].nunique()
                                st.metric("Institutions", unique_institutions)
                            
                            st.markdown("### Study Details")
                            for idx, row in df.iterrows():
                                status_color = "🟢" if row['report_status'] == 'FINAL' else "🟡"
                                with st.expander(
                                    f"{status_color} {row['patient_name']} - {row['study']} ({row['study_date']})"
                                ):
                                    col1, col2, col3 = st.columns(3)
                                    with col1:
                                        st.markdown(f"**Patient ID:** {row['patient_id']}")
                                        st.markdown(f"**Study:** {row['study']}")
                                        st.markdown(f"**Study Time:** {row['study_time']}")
                                    with col2:
                                        st.markdown(f"**Institution:** {row['institution_name']}")
                                        st.markdown(f"**Assigned To:** {row['assigned_to'] or 'Not Assigned'}")
                                    with col3:
                                        st.markdown(f"**Report Status:** {row['report_status']}")
                                        st.markdown(f"**Finalized By:** {row['report_finalized_by'] or 'Pending'}")
                            
                            if st.checkbox("Show Table View", key="worklist_table"):
                                st.dataframe(df, use_container_width=True)
                            
                            csv = df.to_csv(index=False)
                            st.download_button("Download Worklist", csv, "worklist_data.csv", "text/csv")
                        else:
                            st.info("No records found")
                    else:
                        st.error("Failed to load data")
                except Exception as e:
                    st.error(f"Error: {str(e)}")
    
    # TAB 10: Comprehensive View (NEW)
    with tab10:
        st.subheader("Comprehensive Patient View - All Files Combined")
        
        st.markdown("""
        View complete patient information across:
        - **HIS**: Government billing and services
        - **RIS**: Private facility records
        - **Scan Detail**: Scan status and modality
        - **Worklist**: Study assignments and reporting
        """)
        
        search_comp = st.text_input(
            "Search Patient Across All Files",
            placeholder="Enter patient name",
            key="comprehensive_search"
        )
        
        col1, col2 = st.columns([1, 1])
        with col1:
            comp_search_btn = st.button("Search", type="primary", key="btn_comp_search")
        with col2:
            comp_all_btn = st.button("Show All (Top 50)", key="btn_comp_all")
        
        if comp_search_btn or comp_all_btn:
            with st.spinner("Loading comprehensive data..."):
                try:
                    search_param = "" if comp_all_btn else search_comp
                    response = requests.get(
                        f"{BACKEND_URL}/analytics/comprehensive-patient-view",
                        params={"patient_name": search_param}
                    )
                    
                    if response.status_code == 200:
                        data = response.json()['data']
                        df = pd.DataFrame(data)
                        
                        if len(df) > 0:
                            st.success(f"Found {len(df)} patients")
                            
                            for idx, row in df.iterrows():
                                with st.expander(f"👤 {row['patient_name']} (ID: {row['patient_id']})"):
                                    tab_his, tab_ris, tab_scan, tab_work = st.tabs([
                                        "HIS", "RIS", "Scan Detail", "Worklist"
                                    ])
                                    
                                    with tab_his:
                                        st.markdown(f"**Services:** {row['his_services'] or 'N/A'}")
                                        st.metric("Service Count", row['his_service_count'] or 0)
                                    
                                    with tab_ris:
                                        col1, col2 = st.columns(2)
                                        with col1:
                                            st.markdown(f"**Order Type:** {row['ris_order_type'] or 'N/A'}")
                                            st.markdown(f"**Tests:** {row['ris_tests'] or 'N/A'}")
                                        with col2:
                                            st.markdown(f"**Modality:** {row['ris_modality'] or 'N/A'}")
                                            st.metric("Test Count", row['ris_test_count'] or 0)
                                    
                                    with tab_scan:
                                        col1, col2 = st.columns(2)
                                        with col1:
                                            st.markdown(f"**Order Type:** {row['scan_order_type'] or 'N/A'}")
                                            st.markdown(f"**Modality:** {row['scan_modality'] or 'N/A'}")
                                        with col2:
                                            st.markdown(f"**Scan Status:** {row['scan_status'] or 'N/A'}")
                                            st.markdown(f"**Order Status:** {row['order_status'] or 'N/A'}")
                                    
                                    with tab_work:
                                        col1, col2 = st.columns(2)
                                        with col1:
                                            st.markdown(f"**Study:** {row['study'] or 'N/A'}")
                                            st.markdown(f"**Institution:** {row['institution_name'] or 'N/A'}")
                                            st.markdown(f"**Study Time:** {row['study_time'] or 'N/A'}")
                                        with col2:
                                            st.markdown(f"**Assigned To:** {row['assigned_to'] or 'Not Assigned'}")
                                            st.markdown(f"**Finalized By:** {row['report_finalized_by'] or 'Pending'}")
                                            st.markdown(f"**Report Status:** {row['report_status'] or 'N/A'}")
                            
                            if st.checkbox("Show Full Data Table", key="comp_table"):
                                st.dataframe(df, use_container_width=True)
                            
                            csv = df.to_csv(index=False)
                            st.download_button("Download Comprehensive Data", csv, "comprehensive_view.csv", "text/csv")
                        else:
                            st.info("No patients found")
                    else:
                        st.error("Failed to load data")
                except Exception as e:
                    st.error(f"Error: {str(e)}")




# Footer
st.markdown("---")
st.markdown("Built for Hospital Data Analysis | NLP to SQL with Ollama SQLCoder")


