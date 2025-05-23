import streamlit as st 
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential
import pyarrow.parquet as pq
import io


# Authentication configuration
@st.cache_resource
def get_credentials():
    # Method 1: Service Principal authentication (most secure)
    tenant_id = st.secrets["azure_credentials"]["tenant_id"]
    client_id = st.secrets["azure_credentials"]["client_id"]
    client_secret = st.secrets["azure_credentials"]["client_secret"]
    
    credential = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret
    )
    return credential

# Initialize the Data Lake Service Client
@st.cache_resource
def get_datalake_service_client():
    credential = get_credentials()
    storage_account_name = st.secrets["azure_storage"]["account_name"]
    service_client = DataLakeServiceClient(
        account_url=f"https://{storage_account_name}.dfs.core.windows.net/",
        credential=credential
    )
    return service_client

# Function to read CSV files from ADLS Gen2
@st.cache_data
def read_csv_from_adls(container_name, file_path):
    try:
        # Get the service client
        service_client = get_datalake_service_client()
        
        # Get a file system client
        file_system_client = service_client.get_file_system_client(file_system=container_name)
        
        # Get a file client
        file_client = file_system_client.get_file_client(file_path)
        
        # Download the file content
        download = file_client.download_file()
        downloaded_bytes = download.readall()
        
        # Convert to a pandas dataframe
        df = pd.read_csv(io.BytesIO(downloaded_bytes))
        return df
    
    except Exception as e:
        st.error(f"Error reading file from ADLS: {e}")
        return None
    
# Function to read Parquet files from ADLS Gen2
@st.cache_data
def read_parquet_from_adls(container_name, file_path):
    try:
        # Get the service client
        service_client = get_datalake_service_client()
        
        # Get a file system client
        file_system_client = service_client.get_file_system_client(file_system=container_name)
        
        # Get a file client
        file_client = file_system_client.get_file_client(file_path)
        
        # Download the file content
        download = file_client.download_file()
        downloaded_bytes = download.readall()
        
        # Convert to a pandas dataframe using pyarrow
        buffer = io.BytesIO(downloaded_bytes)
        df = pq.read_table(buffer).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error reading Parquet file from ADLS: {e}")
        return None