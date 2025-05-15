import streamlit as st 
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential
from config import read_parquet_from_adls
import io

# Configuration - store these securely in Streamlit secrets or environment variables
st.set_page_config(page_title="Store Performance Analysis", layout="wide")


    
# Streamlit app layout
st.title("Global Fashion Store Performance Analysis")

# Sidebar for navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Select a Page", ["dashboard", "Store Overview"])

# Main content area
if page == "dashboard":

    st.header("dashboard")

    st.title("Power BI Dashboard Integration")

    # Get the embed code from Power BI
    powerbi_embed_code = """
    <iframe title="retail_analytics" width="1024" height="1060" src="https://app.powerbi.com/view?r=eyJrIjoiNWUwMTIwNmEtZTA5Yi00ZWUxLWJkMDQtZGZhN2RjMzlkMmMxIiwidCI6IjAyMDQ1YjNiLTk3OTAtNDAwOC1iODNjLWQxNTU1NzZlNmM3ZSIsImMiOjh9" frameborder="0" allowFullScreen="true"></iframe> 
    """

    # Display using components
    st.components.v1.html(powerbi_embed_code, width=None, height=800)

   
elif page == "Store Overview":
    
    st.header("Store Overview")
    
    # Load sales data from ADLS Gen2
    container_name = "global-fashion-retails-data"
    file_path = "gold/stores/part-00000-109416df-e65b-4273-aca5-c0ec4a3ca534.c000.snappy.parquet"  # Using the gold layer for analytics
    
    with st.spinner("Loading sales data..."):
        store_df = read_parquet_from_adls(container_name, file_path)
    
    if store_df is not None:
        # Display some basic statistics
        st.subheader("Stores Table")
        st.dataframe(store_df)
        
    else:
        st.error("Unable to load sales data. Please check your connection to Azure Data Lake.")

# Additional pages would follow a similar pattern...
