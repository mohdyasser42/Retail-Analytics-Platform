import streamlit as st 
import pandas as pd
import pyodbc
import pandas.io.sql as psql
from config import read_parquet_from_adls, fetch_products_data
import io


# Configuration - store these securely in Streamlit secrets or environment variables
st.set_page_config(page_title="Product", layout="wide")

    
# Removal of anchor icon from Streamlit Headers
st.html("<style>[data-testid='stHeaderActionElements'] {display: none;}</style>")


# Streamlit app layout
header1, header2 = st.columns([0.8,0.2], vertical_alignment="bottom")

header1.title("Global Fashion Retails")

with header2:
    st.link_button("Logout", "https://globalfashionretails.azurewebsites.net/.auth/logout")

st.header("Product Performance Analysis")


# Create tabs for different views
tab1, tab2 = st.tabs(["Dashboard", "Products Mix Overview"])

# Main content area
with tab1:

    st.subheader("Product Analysis Dashboard")

    # Get the embed code from Power BI
    powerbi_embed_code = """
    <div style="display: flex; justify-content: center; width: 100%; height: 100%">
        <iframe title="Products" width="1024" height="1500" src="https://app.powerbi.com/view?r=eyJrIjoiZTY1Njc0NjMtZjcxOC00NzgyLTliZDItNjA3Y2IyYWQwNGNjIiwidCI6IjAyMDQ1YjNiLTk3OTAtNDAwOC1iODNjLWQxNTU1NzZlNmM3ZSIsImMiOjh9&pageName=0a03ef24ee7822886499" frameborder="0" allowFullScreen="true"></iframe>
    </div>
    """

    # Display using components
    st.components.v1.html(powerbi_embed_code, width=None, height=1400)

   
with tab2:
    
    st.subheader("Products Overview")
    
    # Load sales data from ADLS Gen2
    container_name = st.secrets["azure_storage"]["container_name"]
    file_path = st.secrets["azure_storage"]["products_file_path"]
    
    with st.spinner("Loading Products data..."):
        products_df = fetch_products_data()
    
    if not products_df.empty:  
        # Store Overview 
        columns_to_display = ["ProductID", "Category", "SubCategory", "DescriptionEN", "Color", "Sizes", "ProductionCost", "TotalQuantitySold","UniqueCustomers", "TotalRevenue"]

        df = products_df[columns_to_display]

        # Columns for Filters
        ft1, ft2 = st.columns(2)
                
        with ft1:
            # Add filter for country
            category_filter = st.selectbox(
                "Select A Category",
                sorted(df["Category"].unique().tolist())
            )

            # Filter the data based on selection
            if category_filter:
                filtered_df = df[df["Category"] == category_filter]

        with ft2:
            subcategory_filter = st.selectbox(
                "Select A Subcategory",
                sorted(filtered_df["SubCategory"].unique().tolist())
            )

        if subcategory_filter:
            final_df = filtered_df[filtered_df["SubCategory"] == subcategory_filter]
            final_df = final_df.sort_values(by=['DescriptionEN'])


        if final_df is not None:
            with st.spinner("Loading Products data..."):
                # Display the Table
                st.dataframe(final_df,
                                use_container_width=True,
                                hide_index=True,
                                column_config={
                                    "ProductID": "Product ID",
                                    "DescriptionEN": "Product Name",
                                    "ProductionCost": st.column_config.NumberColumn(
                                        "Production Cost",
                                        format="$%.2f"
                                    ),
                                    "TotalQuantitySold": "Quantity Sold",
                                    "UniqueCustomers": "Buyers Count",
                                    "TotalRevenue": st.column_config.NumberColumn(
                                        "Net Revenue",
                                        format="$%.2f"
                                    )
                                }
                                )
    else:
        st.error("Unable to load sales data. Please check your connection to Azure SQL Database.")

# Additional pages would follow a similar pattern...