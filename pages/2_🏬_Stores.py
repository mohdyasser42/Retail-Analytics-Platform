import streamlit as st 
from config import read_parquet_from_adls


# Configuration - store these securely in Streamlit secrets or environment variables
st.set_page_config(page_title="Dashboard", layout="wide")

# Removal of anchor icon from Streamlit Headers
st.html("<style>[data-testid='stHeaderActionElements'] {display: none;}</style>")


# Streamlit app layout
header1, header2 = st.columns([0.8,0.1], vertical_alignment="bottom")

header1.title("Global Fashion Retails")

with header2:
    st.link_button("Logout", "https://globalfashionretails.azurewebsites.net/.auth/logout")

st.header("Store Performance Analysis")

# Load sales data from ADLS Gen2
container_name = st.secrets["azure_storage"]["container_name"]
file_path = st.secrets["azure_storage"]["stores_file_path"]
store_df = None

with st.spinner("Loading Stores data..."):
    store_df = read_parquet_from_adls(container_name, file_path)
    
if store_df is not None:

    # Changing the Column Names and Datatype Suitable for Streamlit in-built functions
    store_df = store_df.rename(columns={'Latitude': 'latitude', 'Longitude': 'longitude'})
    store_df['latitude'] = store_df['latitude'].astype(float)
    store_df['longitude'] = store_df['longitude'].astype(float)
        

# Create tabs for different views
tab1, tab2, tab3, tab4 = st.tabs(["Dashboard", "Overview", "Store Profile", "Geographic Locations"])

# Main content area
with tab1:
    st.subheader("Analysis Dashboard")

    # Get the embed code from Power BI
    powerbi_embed_code = """
    <div style="display: flex; justify-content: center; width: 100%; height: 100%">
        <iframe title="Stores" width="1024" height="1500" src="https://app.powerbi.com/view?r=eyJrIjoiNGVhNzhlZTUtNjRkZi00ZTIwLTgxYTEtMDM0NmIxYWY0MTZhIiwidCI6IjAyMDQ1YjNiLTk3OTAtNDAwOC1iODNjLWQxNTU1NzZlNmM3ZSIsImMiOjh9&pageName=0a03ef24ee7822886499" frameborder="0" allowFullScreen="true"></iframe>
    </div>
    """

    # Display using components
    st.components.v1.html(powerbi_embed_code, width=None, height=1400)

   
with tab2:
    st.subheader("Store Overview")        
    
    if store_df is not None:
        # Display summary statistics
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Total Stores", len(store_df))
            
            with col2:
                st.metric("Total Employees", store_df["NumberOfEmployees"].sum())
            
            with col3:
                st.metric("Avg Employees per Store", round(store_df["NumberOfEmployees"].mean(), 1))
            
            # Group by Country and calculate the metrics
            country_metrics = store_df.groupby('Country').agg(
                StoreCount=('Country', 'count'),
                CountryTotalSales=('TotalSalesUSD', 'sum'),
                AvgStoreRevenue=('TotalSalesUSD', 'mean'),
                AvgMonthlyRevenue=('AverageMonthlyUSD', 'mean'),
                AvgReturnRate=('ReturnRate', 'mean')
            )

            # Sort by CountryTotalSales in descending order
            country_metrics = country_metrics.sort_values('CountryTotalSales', ascending=False).reset_index()

            # Display the Analysis
            st.subheader("Stores Table")
            st.dataframe(country_metrics, hide_index=True)
    else:
        st.error("Unable to load sales data. Please check your connection to Azure Data Lake Storage.")


    
# Store Details Tab
with tab3:
    st.subheader("Store Profile")

    if store_df is not None:

        # Columns for Filters
        ft1, ft2 = st.columns(2)
                
        with ft1:
            # Add filter for country
            country_filter = st.selectbox(
                "Select A Country",
                sorted(store_df["Country"].unique().tolist())
            )

            # Filter the data based on selection
            if country_filter:
                filtered_df = store_df[store_df["Country"] == country_filter]

        with ft2:
            store_filter = st.selectbox(
                "Select A Store",
                sorted(filtered_df["StoreName"].unique().tolist())
            )

        if store_filter:
            final_df = filtered_df[filtered_df["StoreName"] == store_filter]

        st.write("")
        st.write("")
        
        if final_df is not None:
            pcol1, pcol2, pcol3 = st.columns([0.5,3,0.5], border=False)

            with pcol2:
                with st.container(border=True):
                    st.write("")
                    st.write("")

                    logo = """
                        <div style="display: flex; justify-content: center; width: 100%;">
                            <iframe src="https://lottie.host/embed/3163e453-3217-4f49-b752-b48ebd378e04/2e35UCkOoW.lottie" style="border: None;"></iframe>
                        </div>
                    """
                    st.components.v1.html(logo)

                    store_name = final_df.iat[0, final_df.columns.get_loc("StoreName")]
                    store_id = final_df.iat[0, final_df.columns.get_loc("StoreID")]
                    store_country = final_df.iat[0, final_df.columns.get_loc("Country")]
                    store_city = final_df.iat[0, final_df.columns.get_loc("City")]
                    store_zipcode = final_df.iat[0, final_df.columns.get_loc("ZIPCode")]
                    monthsOperation = final_df.iat[0, final_df.columns.get_loc("MonthsOfOperation")]
                    num_employees = final_df.iat[0, final_df.columns.get_loc("NumberOfEmployees")]
                    avgusd = round(final_df.iat[0, final_df.columns.get_loc("AverageMonthlyUSD")],2)
                    sales = round(final_df.iat[0, final_df.columns.get_loc("TotalSalesUSD")],2)
                    transactions = final_df.iat[0, final_df.columns.get_loc("TotalTransactions")]
                    returns = final_df.iat[0, final_df.columns.get_loc("TotalReturns")]
                    returnrate = final_df.iat[0, final_df.columns.get_loc("ReturnRate")]

                    st.markdown(f"<h2 style='text-align: center;'>{store_name}</h2>", unsafe_allow_html=True)
                    st.markdown("---")


                    r1start, r1col1, r1col2, r1col3, r1col4 = st.columns([0.25,1,1,1,1])
                    st.write("")
                    st.write("")
                    r2start, r2col1, r2col2, r2col3 = st.columns([0.25,1,0.8,1.2])
                    st.write("")
                    st.write("")
                    r3start, r3col1, r3col2, r3col3, r3col4 = st.columns([0.25,1,1,1,1])
                    st.write("")
                    st.write("")

                    r1col1.markdown(f"<h5 style=''>Store ID</h5>", unsafe_allow_html=True)
                    r1col1.markdown(f"<p style=''>{store_id}</p>", unsafe_allow_html=True)
                    
                    r1col2.markdown(f"<h5 style=''>Country</h5>", unsafe_allow_html=True)
                    r1col2.markdown(f"<p style=''>{store_country}</p>", unsafe_allow_html=True)

                    r1col3.markdown(f"<h5 style=''>City</h5>", unsafe_allow_html=True)
                    r1col3.markdown(f"<p style=''>{store_city}</p>", unsafe_allow_html=True)

                    r1col4.markdown(f"<h5 style=''>ZipCode</h5>", unsafe_allow_html=True)
                    r1col4.markdown(f"<p style=''>{store_zipcode}</p>", unsafe_allow_html=True)
                    
                    r2col1.markdown(f"<h5 style=''>Months Of Operation</h5>", unsafe_allow_html=True)
                    r2col1.markdown(f"<p style=''>{monthsOperation}</p>", unsafe_allow_html=True)
                    
                    r2col2.markdown(f"<h5 style=''>Total Employees</h5>", unsafe_allow_html=True)
                    r2col2.markdown(f"<p style=''>{num_employees}</p>", unsafe_allow_html=True)
                    
                    r2col3.markdown(f"<h5 style=''>Avg Monthly Sales(USD)</h5>", unsafe_allow_html=True)
                    r2col3.markdown(f"<p style=''>{avgusd}</p>", unsafe_allow_html=True)

                    r3col1.markdown(f"<h5 style=''>Net Revenue(USD)</h5>", unsafe_allow_html=True)
                    r3col1.markdown(f"<p style=''>{sales}</p>", unsafe_allow_html=True)

                    r3col2.markdown(f"<h5 style=''>Total Transactions</h5>", unsafe_allow_html=True)
                    r3col2.markdown(f"<p style=''>{transactions}</p>", unsafe_allow_html=True)

                    r3col3.markdown(f"<h5 style=''>Total Refunds</h5>", unsafe_allow_html=True)
                    r3col3.markdown(f"<p style=''>{returns}</p>", unsafe_allow_html=True)

                    r3col4.markdown(f"<h5 style=''>Rate Of Return</h5>", unsafe_allow_html=True)
                    r3col4.markdown(f"<p style=''>{returnrate}</p>", unsafe_allow_html=True)
    else:
        st.error("Unable to load sales data. Please check your connection to Azure Data Lake.")

            

# Geographic Analysis Tab
with tab4:
    st.subheader("Geographic Distribution")

    if store_df is not None:
    
        # Prepare data for map
        map_data = store_df[["StoreName", "City", "Country", "NumberOfEmployees", "latitude", "longitude"]]
        
        # Display interactive map
        st.map(map_data)
        
        # Add a note about store locations
        st.info("""
        The Map Shows the Geographic Distribution of all Global Fashion Retail Stores Across Globe.
        """)
        
        # Option to download store location data
        csv = map_data.to_csv(index=False)
        st.download_button(
            label="Download Store Locations",
            data=csv,
            file_name="store_locations.csv",
            mime="text/csv",
        ) 
    else:
        st.error("Unable to load sales data. Please check your connection to Azure Data Lake.") 


