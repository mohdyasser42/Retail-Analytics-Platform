import streamlit as st 
from config import read_parquet_from_adls


# Configuration - store these securely in Streamlit secrets or environment variables
st.set_page_config(page_title="Executive", layout="wide")

    
# Streamlit app layout
header1, header2 = st.columns([0.8,0.1], vertical_alignment="bottom")

header1.title("Global Fashion Retails")

with header2:
    st.link_button("Logout", "https://globalfashionretails.azurewebsites.net/.auth/logout")

st.header("Revenue and Profitability Analysis")

executive_dashboard = st.container()

with executive_dashboard:
    # Main content area
    st.subheader("Analysis Dashboard")
    # Get the embed code from Power BI
    powerbi_embed_code = """
    <div style="display: flex; justify-content: center; width: 100%; height: 100%">
        <iframe title="Executive" width="1024" height="1500" src="https://app.powerbi.com/view?r=eyJrIjoiZDlhOWQzNDItMjQyYy00YTdlLThmMjItZGZmNjNjMzA5MGY0IiwidCI6IjAyMDQ1YjNiLTk3OTAtNDAwOC1iODNjLWQxNTU1NzZlNmM3ZSIsImMiOjh9&pageName=0a03ef24ee7822886499" frameborder="0" allowFullScreen="true"></iframe>
    </div>
    """

    # Display using components
    st.components.v1.html(powerbi_embed_code, width=None, height=1400)
