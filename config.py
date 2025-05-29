import streamlit as st 
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from urllib.parse import quote
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
import os
import io

# Get 
@st.cache_resource
def get_client_secret():
    keyVaultName = "gfr-key-vault"
    KVUri = f"https://gfr-key-vault.vault.azure.net/"
    secretName = "db-client-secret"

    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=KVUri, credential=credential)
    retrieved_secret = client.get_secret(secretName)
    return retrieved_secret


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
    

# Database Connection
@st.cache_resource  # Cache the engine, not the connection string
def get_database_engine():
    connection_url = URL.create(
        "mssql+pyodbc",
        username=st.secrets["sql_connection"]["USERNAME"],
        password=st.secrets["sql_connection"]["PASSWORD"],
        host=st.secrets["sql_connection"]["SERVER"],
        database=st.secrets["sql_connection"]["DATABASE"],
        query={
            "driver": "ODBC Driver 17 for SQL Server",
            "Encrypt": "yes",
            "TrustServerCertificate": "yes",
            "Connection Timeout": "120",
            "Command Timeout": "300",
            "ConnectRetryCount": "3",
            "ConnectRetryInterval": "10"
            }
    )
    
    return create_engine(
        connection_url,
        pool_size=5,          # Connection pooling
        max_overflow=10,      # Extra connections if needed
        pool_timeout=30,
        pool_recycle=3600     # Refresh connections hourly
    )

@st.cache_data(ttl=86400)  # Cache for 24 hours
def fetch_products_data():
    """
    Fetch products data with sales metrics from transactions
    """
    engine = get_database_engine()
    
    query = '''
    SELECT 
        p.ProductID,
        p.Category,
        p.SubCategory,
        p.DescriptionEN,
        p.Color,
        p.Sizes,
        p.ProductionCost,
        
        -- Sales metrics from transactions
        COALESCE(sales.TotalQuantitySold, 0) as TotalQuantitySold,
        COALESCE(sales.TotalRevenue, 0) as TotalRevenue,
        COALESCE(sales.UniqueCustomers, 0) as UniqueCustomers        
    FROM [GlobalFashion].[Products] p
    LEFT JOIN (
        SELECT 
            ProductID,
            SUM(Quantity) as TotalQuantitySold,
            SUM(LineTotalUSD) as TotalRevenue,
            COUNT(DISTINCT CustomerID) as UniqueCustomers
        FROM [GlobalFashion].[InvoiceLineItems]
        GROUP BY ProductID
    ) sales ON p.ProductID = sales.ProductID
    
    ORDER BY COALESCE(sales.TotalQuantitySold, 0) DESC
    '''
    
    return pd.read_sql(query, engine)


@st.cache_data(ttl=86400)
def fetch_customer_byid(id):  
    engine = get_database_engine()
    query = f"SELECT * FROM [GlobalFashion].[Customers] WHERE CustomerID = {id}"
    return pd.read_sql(query, engine)

@st.cache_data(ttl=86400)
def fetch_customers_byname(name):  
    engine = get_database_engine()
    query = f"SELECT TOP 20 CustomerID, Name, Gender, City, Country FROM [GlobalFashion].[Customers] WHERE Name LIKE '{name}%'"
    return pd.read_sql(query, engine)

@st.cache_data(ttl=86400)
def fetch_customer_byemail(email):  
    engine = get_database_engine()
    query = f"SELECT TOP 1 * FROM [GlobalFashion].[Customers] WHERE Email = '{email}'"
    return pd.read_sql(query, engine)

@st.cache_data(ttl=86400)
def fetch_customer_bytelephone(telephone):  
    engine = get_database_engine()
    query = f"SELECT TOP 1 * FROM [GlobalFashion].[Customers] WHERE Telephone = '{telephone}'"
    return pd.read_sql(query, engine)

@st.cache_data(ttl=86400)
def fetch_invoice_fact_data(id):  
    engine = get_database_engine()
    query = f"SELECT [InvoiceID], [CustomerID], [Date], [Time], [StoreID], [StoreCountry], [StoreCity], [EmployeeID], [TotalQuantity], [Currency], [CurrencySymbol], [TransactionType], [PaymentMethod], [InvoiceTotal], [InvoiceTotalUSD] FROM [GlobalFashion].[InvoiceFact] Where CustomerID = {id} ORDER BY [Date] DESC, [Time] DESC"
    return pd.read_sql(query, engine)

@st.cache_data(ttl=86400)
def fetch_invoice_line_items_data(id):  
    engine = get_database_engine()
    query = f"SELECT * FROM [GlobalFashion].[InvoiceLineItems] Where CustomerID = {id} ORDER BY [Date] DESC"
    return pd.read_sql(query, engine)

