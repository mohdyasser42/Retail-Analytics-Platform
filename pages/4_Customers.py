import streamlit as st 
import pandas as pd
from config import read_parquet_from_adls
import io

# Configuration - store these securely in Streamlit secrets or environment variables
st.set_page_config(page_title="Customers", layout="wide")

    
# Removal of anchor icon from Streamlit Headers
st.html("<style>[data-testid='stHeaderActionElements'] {display: none;}</style>")


# Streamlit app layout
st.title("Global Fashion Retails")

# Create tabs for different views
tab1, tab2 = st.tabs(["Dashboard", "Customers Overview"])

# Main content area
with tab1:

    st.subheader("Customer Analysis Dashboard")

    # Get the embed code from Power BI
    powerbi_embed_code = """
    <div style="display: flex; justify-content: center; width: 100%; height: 100%">
        <iframe title="Customers" width="1024" height="1500" src="https://app.powerbi.com/view?r=eyJrIjoiMTZjODQ5YTYtYmY3OS00NjEyLTliNGEtNWI1YTg1MTlhZGNkIiwidCI6IjAyMDQ1YjNiLTk3OTAtNDAwOC1iODNjLWQxNTU1NzZlNmM3ZSIsImMiOjh9&pageName=0a03ef24ee7822886499" frameborder="0" allowFullScreen="true"></iframe>
    </div>
    """

    # Display using components
    st.components.v1.html(powerbi_embed_code, width=None, height=1400)

   
with tab2:
    
    st.subheader("Customers Overview")
    
    # Load sales data from ADLS Gen2
    container_name = "global-fashion-retails-data"
    customer_file_path = "gold/customers/part-00000-2bbf662b-cab3-4bbd-b4fc-cbbe0da81410.c000.snappy.parquet"
    transactions_file_path = "gold/invoice_line_items/part-00000-027b79af-fe50-48f8-b3f7-4c7e4cdb64fd.c000.snappy.parquet"
    
    with st.spinner("Loading Customers data..."):
        customers_df = read_parquet_from_adls(container_name, customer_file_path)
    
    with st.spinner("Loading Transactions data..."):
        invoice_line_df = read_parquet_from_adls(container_name, transactions_file_path)
    
    if customers_df is not None:  

        # Store Overview 
        if customers_df is not None:
            form = st.form(key='id_query', clear_on_submit=True)
            id_qry = form.number_input("Enter Customer ID:", value=None, step=1 )
            submit = form.form_submit_button('Search')
            
            if submit:
                if not id_qry:  
                    st.error("Enter a valid ID")
                else:
                    st.write(f"Query: {id_qry}.")
                    qry_result = customers_df[customers_df['CustomerID'] == id_qry]
                    # Display the Table
                    st.dataframe(qry_result, use_container_width=True)
            
    if invoice_line_df is not None:
        st.write("Working")
    else:
        st.error("Unable to load sales data. Please check your connection to Azure Data Lake.")
