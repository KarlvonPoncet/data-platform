import streamlit as st
import duckdb
import pandas as pd
import os
import plotly.express as px
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Page Configuration
st.set_page_config(
    page_title="Wiener Linien Monitor",
    page_icon="🚋",
    layout="wide"
)

# Constants
BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "traffic-data")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

@st.cache_resource
def get_db_connection():
    """
    Establishes a connection to DuckDB and configures MinIO access.
    """
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL json; LOAD json;")
    con.execute(f"""
        SET s3_endpoint='{MINIO_ENDPOINT}';
        SET s3_access_key_id='{MINIO_ACCESS_KEY}';
        SET s3_secret_access_key='{MINIO_SECRET_KEY}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)
    return con

def load_data(con):
    """
    Loads the latest refined data from MinIO.
    """
    try:
        # Load all refined parquet files
        query = f"""
            SELECT *
            FROM read_parquet('s3://{BUCKET_NAME}/refined/*.parquet')
        """
        df = con.execute(query).fetchdf()
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()

# Title
st.title("🚋 Wiener Linien Realtime Monitor")

# Connection
con = get_db_connection()

# Load Data
with st.spinner("Loading data from MinIO..."):
    df = load_data(con)

if not df.empty:
    # Sidebar Filters
    st.sidebar.header("Filters")
    
    # Time Slider (if multiple snapshots exist, maybe select by range)
    if 'ingestion_timestamp' in df.columns:
        df['ingestion_timestamp'] = pd.to_datetime(df['ingestion_timestamp'])
        min_time = df['ingestion_timestamp'].min()
        max_time = df['ingestion_timestamp'].max()
        
        # Determine unique snapshots
        snapshots = df['ingestion_timestamp'].sort_values(ascending=False).unique()
        selected_snapshot = st.sidebar.selectbox("Select Snapshot", snapshots, index=0)
        
        # Filter by snapshot
        df_filtered = df[df['ingestion_timestamp'] == selected_snapshot].copy()
    else:
        df_filtered = df.copy()

    # Station Filter
    all_stations = sorted(df_filtered['station_name'].dropna().unique())
    selected_stations = st.sidebar.multiselect("Select Stations", all_stations)
    
    if selected_stations:
        df_filtered = df_filtered[df_filtered['station_name'].isin(selected_stations)]

    # Line Filter
    all_lines = sorted(df_filtered['line'].unique())
    selected_lines = st.sidebar.multiselect("Select Lines", all_lines)
    
    if selected_lines:
        df_filtered = df_filtered[df_filtered['line'].isin(selected_lines)]

    # KPI Metrics
    st.markdown("### Current Status Overview")
    kpi1, kpi2, kpi3 = st.columns(3)
    
    total_events = len(df_filtered)
    avg_countdown = df_filtered['countdown'].mean()
    
    kpi1.metric("Total Departures", total_events)
    kpi2.metric("Avg Countdown", f"{avg_countdown:.1f} min")
    
    # Real-time Table
    st.markdown("### Detailed Departures")
    
    # Color code countdown
    def color_countdown(val):
        color = 'red' if val < 2 else 'green' if val > 5 else 'orange'
        return f'color: {color}'

    st.dataframe(
        df_filtered[['station_name', 'line', 'direction', 'time_planned', 'time_real', 'countdown']]
        .sort_values(by=['station_name', 'countdown'])
        .style.map(color_countdown, subset=['countdown']),
        use_container_width=True
    )
    
    # Charts?
    # Count per line
    st.markdown("### Departures by Line")
    chart_data = df_filtered['line'].value_counts().reset_index()
    chart_data.columns = ['Line', 'Count']
    st.bar_chart(chart_data, x='Line', y='Count')

else:
    st.warning("No data found in the refined layer.")
    st.info("Ensure that the ingestion service has run and transformed data.")
