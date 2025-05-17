import streamlit as st 
import pandas as pd
from config import read_parquet_from_adls
import io


# Configuration - store these securely in Streamlit secrets or environment variables
st.set_page_config(page_title="Executive", layout="wide")

    
# Streamlit app layout
st.title("Global Fashion Retails")

executive_dashboard = st.container()

with executive_dashboard:
    # Main content area
    st.header("Executive Dashboard")

    # Get the embed code from Power BI
    powerbi_embed_code = """
    <div style="display: flex; justify-content: center; width: 100%;">
        <iframe title="Executive" width="1024" height="1060" src="https://app.powerbi.com/view?r=eyJrIjoiZTcwODY0MzEtNzc5NC00ODc3LThkZWMtMWExMzE0NDkxZDRlIiwidCI6IjAyMDQ1YjNiLTk3OTAtNDAwOC1iODNjLWQxNTU1NzZlNmM3ZSIsImMiOjh9" frameborder="0" allowFullScreen="true"></iframe> 
    </div>
    """

    # Display using components
    st.components.v1.html(powerbi_embed_code, width=None, height=1000)
