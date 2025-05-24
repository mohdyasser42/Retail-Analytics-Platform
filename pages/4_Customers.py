import streamlit as st 
import pandas as pd
from config import fetch_customer_byid, fetch_customers_byname, fetch_customer_byemail, fetch_customer_bytelephone, fetch_invoice_line_items_data, fetch_invoice_fact_data
import io
import re

# Configuration - store these securely in Streamlit secrets or environment variables
st.set_page_config(page_title="Customers", layout="wide")

    
# Removal of anchor icon from Streamlit Headers
st.html("<style>[data-testid='stHeaderActionElements'] {display: none;}</style>")


# Streamlit app layout
st.title("Global Fashion Retails")
st.link_button("Logout", "https://globalfashionretails.azurewebsites.net/.auth/logout")

# Create tabs for different views
tab1, tab2 = st.tabs(["Dashboard", "Customer Profile"])

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
    
    st.subheader("Customer Profile")
    
    # with st.spinner("Loading Transactions data..."):
    #     invoice_line_df = fetch_invoice_line_items_data()

    # Store Overview 
    qry_result = None
    submit2 = None
    # Assign Columns to align searchby radios and search box in one line
    scol1, scol2 = st.columns(2)
    with scol1:
        searchby = st.radio("Search Customer by:", ["ID", "Name", "Email","Telephone"], horizontal= True)
    
    with scol2:
        if searchby == "ID":
            
            form1 = st.form(key='id_query', clear_on_submit=True, border=False)
            with form1:
                formcol1, formcol2 = st.columns([3,1], vertical_alignment="bottom")
                with formcol1:
                    id_qry = st.number_input("Enter Customer ID:", help= "Enter a Valid ID in Integer.", value=None, step=1,  placeholder="Customer ID" )
                with formcol2:
                    submit1 = st.form_submit_button('Search', use_container_width=True, help="Seach")

            if submit1:
                if not id_qry:  
                    st.error("Enter a valid ID")
                else:
                    with st.spinner("Loading Customers data..."):
                        qry_result = fetch_customer_byid(id_qry)
                    if qry_result.empty:
                        st.write("No Record Found...")
        
        elif searchby == "Name":

            form2 = st.form(key='name_query', clear_on_submit=True, border=False)
            with form2:
                formcol1, formcol2 = st.columns([3,1], vertical_alignment="bottom")
                with formcol1:
                    name_qry = st.text_input("Enter Customer Name:", value=None, max_chars = 30, placeholder="Customer Name" )
                with formcol2:
                    submit2 = st.form_submit_button('Search', use_container_width=True, help="Seach")

            # Initialize session state
            if 'customer_id' not in st.session_state:
                st.session_state.customer_id = None

            def call():
                if 'customer_table' in st.session_state:
                    if st.session_state.customer_table['selection']['rows']:
                        selected_row_index = st.session_state.customer_table['selection']['rows'][0]
                        customer_id = name_result.iloc[selected_row_index]['CustomerID']
                        st.session_state.customer_id = customer_id
                    else:
                        st.session_state.customer_id = None
            
            if submit2:
                if not name_qry:  
                    st.error("Enter a valid Name")
                else:
                    st.write(f"**Search Results for:** {name_qry}")
                    with st.spinner("Loading Customers data..."):
                        name_result = fetch_customers_byname(name_qry)
                    
                    if name_result.empty:
                        st.write("No Record Found...")
                    else:
                        # Display the Table
                        st.markdown("**Select Customer:**")
                        st.dataframe(
                            name_result,
                            use_container_width=True,
                            hide_index= True,
                            on_select=call,
                            selection_mode="single-row",
                            key="customer_table"  
                        )
            
            # Display the selected customer ID
            cus_id = st.session_state.customer_id
            if cus_id:
                with st.spinner("Loading Customers data..."):
                    qry_result = fetch_customer_byid(cus_id)
                if qry_result.empty:
                    st.write("No Record Found...")

        elif searchby == "Email":
            form3 = st.form(key='email_query', clear_on_submit=True, border=False)
            with form3:
                formcol1, formcol2 = st.columns([3,1], vertical_alignment="bottom")
                with formcol1:
                    email_qry = st.text_input("Enter Customer Email:", help = "Enter a valid Email with one '@' and atleast one '.'", value=None, max_chars = 30, placeholder="Customer Email")
                with formcol2:
                    submit3 = st.form_submit_button('Search', use_container_width=True, help="Seach")

            if submit3:
                if not email_qry:  
                    st.error("Enter a Email:")
                elif not ('@' in email_qry and '.' in email_qry and email_qry.count('@') == 1):
                    st.error("Enter a Valid Email (must contain @ and '.')")
                else:
                    with st.spinner("Loading Customers data..."):
                        qry_result = fetch_customer_byemail(email_qry)
                    if qry_result.empty:
                        st.write("No Record Found...")
                    
        elif searchby == "Telephone":
            form4 = st.form(key='telephone_query', clear_on_submit=True, border=False)
            with form4:
                formcol1, formcol2 = st.columns([3,1], vertical_alignment="bottom")
                with formcol1:
                    phno_qry = st.text_input("Enter Customer Telephone", help = "Enter a Telephone with Country Code and phone number.", value=None, max_chars = 40, placeholder="Customer Telephone")
                with formcol2:
                    submit4 = st.form_submit_button('Search', use_container_width=True, help="Seach")

            if submit4:
                if not phno_qry:  
                    st.error("Enter a Telephone:")
                elif not phno_qry.startswith('+'):
                    st.error("Enter a Valid Telephone with Country Code (must start with +)")
                elif len(phno_qry) < 8:  
                    st.error("Phone number too short")
                elif not re.match(r'^\+\d+$', phno_qry):
                    st.error("Phone number contains invalid characters")
                else:
                    with st.spinner("Loading Customers data..."):
                        qry_result = fetch_customer_bytelephone(phno_qry)
                    if qry_result.empty:
                        st.write("No Record Found...")
    

             
    if qry_result is not None and not qry_result.empty:
        c_ID = qry_result.iat[0, qry_result.columns.get_loc("CustomerID")]
        c_Name = qry_result.iat[0, qry_result.columns.get_loc("Name")]
        c_Email = qry_result.iat[0, qry_result.columns.get_loc("Email")]
        c_Telephone = qry_result.iat[0, qry_result.columns.get_loc("Telephone")]
        c_City = qry_result.iat[0, qry_result.columns.get_loc("City")]
        c_Country = qry_result.iat[0, qry_result.columns.get_loc("Country")]
        c_Gender = qry_result.iat[0, qry_result.columns.get_loc("Gender")]
        c_DateOfBirth = qry_result.iat[0, qry_result.columns.get_loc("DateOfBirth")]
        c_JobTitle = qry_result.iat[0, qry_result.columns.get_loc("JobTitle")]
        c_Age = qry_result.iat[0, qry_result.columns.get_loc("Age")]
        c_IsParent = qry_result.iat[0, qry_result.columns.get_loc("IsParent")]
        c_TotalSpending = qry_result.iat[0, qry_result.columns.get_loc("TotalSpending")]
        c_NoOfInvoices = qry_result.iat[0, qry_result.columns.get_loc("NoOfInvoices")]
        c_AverageSpending = qry_result.iat[0, qry_result.columns.get_loc("AverageSpending")]
        c_RFM_Segment = qry_result.iat[0, qry_result.columns.get_loc("RFM_Segment")]
        c_Recency = qry_result.iat[0, qry_result.columns.get_loc("Recency")]

        M_logo = """
            <div style="display: flex; justify-content: center; width: 100%;">
                <iframe src="https://lottie.host/embed/cd706444-ff52-43e1-80df-bcd0753e21b5/v8Z4O7adxF.lottie" style="border: None;"></iframe>
            </div>
        """
        F_logo = """
            <div style="display: flex; justify-content: center; width: 100%;">
                <iframe src="https://lottie.host/embed/85f06b52-eb07-4bf3-87d4-afd7c3249926/8fI7RHYjKC.lottie" style="border: None;"></iframe>
            </div>
        """
        D_logo = """
            <div style="display: flex; justify-content: center; width: 100%;">
                <iframe src="https://lottie.host/embed/38b82eaa-6a66-4767-8eba-026b0f0e2016/VA8YMg0XGk.lottie" style="border: None;"></iframe>
            </div>
        """
        inv_logo = """
            <div style="display: flex; justify-content: center; width: 100%; ">
                <iframe src="https://lottie.host/embed/bde9a628-3e7d-4031-acf5-dd4eff80155f/35mpdp4z4v.lottie" style="border: None;"></iframe>
            </div>
        """
        st.write("")
        st.write("")
        pcol1, pcol2, pcol3 = st.columns([0.05,1,0.05])
        with pcol2:
            with st.container(border=True):
                st.write("")

                if c_Gender == "M":
                    st.components.v1.html(M_logo)
                elif c_Gender == "F":
                    st.components.v1.html(F_logo)
                else:
                    st.components.v1.html(D_logo)

                st.markdown(f"<h3 style='text-align: center;'>{c_Name}</h3>", unsafe_allow_html=True)
                st.markdown("---")

                r1start, r1col1, r1col2, r1col3, r1col4 = st.columns([0.25,1,1,1.25,0.75])
                st.write("")
                st.write("")
                r2start, r2col1, r2col2, r2col3, r2col4 = st.columns([0.25,1,1,1.25,0.75])
                st.write("")
                st.write("")
                r3start, r3col1, r3col2, r3col3, r3col4 = st.columns([0.25,1,1,1.25,0.75])
                st.write("")
                st.write("")
                r4start, r4col1, r4col2, r4col3 = st.columns([0.25,1,1,1]) 
                st.write("")
                st.write("")


                r1col1.markdown(f"<h5 style=''>Customer ID</h5>", unsafe_allow_html=True)
                r1col1.markdown(f"<p style=''>{c_ID}</p>", unsafe_allow_html=True)

                r1col2.markdown(f"<h5 style=''>Gender</h5>", unsafe_allow_html=True)
                r1col2.markdown(f"<p style=''>{c_Gender}</p>", unsafe_allow_html=True)
                
                r1col3.markdown(f"<h5 style=''>Email</h5>", unsafe_allow_html=True)
                r1col3.markdown(f"<p style=''>{c_Email}</p>", unsafe_allow_html=True)
                
                r1col4.markdown(f"<h5 style=''>Telephone</h5>", unsafe_allow_html=True)
                r1col4.markdown(f"<p style=''>{c_Telephone}</p>", unsafe_allow_html=True)
                
                r2col1.markdown(f"<h5 style=''>Age</h5>", unsafe_allow_html=True)
                r2col1.markdown(f"<p style=''>{c_Age}</p>", unsafe_allow_html=True)
                
                r2col2.markdown(f"<h5 style=''>City</h5>", unsafe_allow_html=True)
                r2col2.markdown(f"<p style=''>{c_City}</p>", unsafe_allow_html=True)
                
                r2col3.markdown(f"<h5 style=''>Country</h5>", unsafe_allow_html=True)
                r2col3.markdown(f"<p style=''>{c_Country}</p>", unsafe_allow_html=True)
                
                r2col4.markdown(f"<h5 style=''>Job Title</h5>", unsafe_allow_html=True)
                r2col4.markdown(f"<p style=''>{c_JobTitle}</p>", unsafe_allow_html=True)
                
                r3col1.markdown(f"<h5 style=''>Date of Birth</h5>", unsafe_allow_html=True)
                r3col1.markdown(f"<p style=''>{c_DateOfBirth}</p>", unsafe_allow_html=True)
                
                r3col2.markdown(f"<h5 style=''>Is Parent</h5>", unsafe_allow_html=True)
                r3col2.markdown(f"<p style=''>{c_IsParent}</p>", unsafe_allow_html=True)
                
                r3col3.markdown(f"<h5 style=''>Value Segment</h5>", unsafe_allow_html=True)
                r3col3.markdown(f"<p style=''>{c_RFM_Segment}</p>", unsafe_allow_html=True)
                
                r3col4.markdown(f"<h5 style=''>Last Purchase</h5>", unsafe_allow_html=True)
                r3col4.markdown(f"<p style=''>{c_Recency} days ago</p>", unsafe_allow_html=True)
                
                r4col1.markdown(f"<h5 style=''>Total Revenue</h5>", unsafe_allow_html=True)
                r4col1.markdown(f"<p style=''>{c_TotalSpending}</p>", unsafe_allow_html=True)
                
                r4col2.markdown(f"<h5 style=''>Total Transactions</h5>", unsafe_allow_html=True)
                r4col2.markdown(f"<p style=''>{c_NoOfInvoices}</p>", unsafe_allow_html=True)
                
                r4col3.markdown(f"<h5 style=''>Avg Revenue per Transaction</h5>", unsafe_allow_html=True)
                r4col3.markdown(f"<p style=''>{c_AverageSpending}</p>", unsafe_allow_html=True)
            
            with st.container(border=True):
                st.markdown(f"<h3 style='text-align: center;'>Transaction Invoice Receipts</h3>", unsafe_allow_html=True)

                with st.spinner("Loading Transactions data..."):
                    c_transaction_df = fetch_invoice_fact_data(c_ID)

                if c_transaction_df.empty:
                    st.write("No Transaction Record Found.....")
                else:
                    for index, row in c_transaction_df.iterrows():
                        st.divider()
                        inv_ID = row["InvoiceID"]
                        inv_cID = row["CustomerID"]
                        inv_date = row["Date"]
                        inv_time = row["Time"]
                        inv_storeID = row["StoreID"]
                        inv_country = row["StoreCountry"]
                        inv_city = row["StoreCity"]
                        inv_empID = row["EmployeeID"]
                        inv_quantity = row["TotalQuantity"]
                        inv_currency = row["Currency"]
                        inv_CSymbol = row["CurrencySymbol"]
                        inv_Ttype = row["TransactionType"]
                        inv_Pmethod = row["PaymentMethod"]
                        inv_total = row["InvoiceTotal"]
                        inv_totalUSD = row["InvoiceTotalUSD"]

                        with st.container(border=True):
                            cont1 = st.container()
                            cont2 = st.container()
                            cont3 = st.container()
                            cont4 = st.container()
                            cont5 = st.container()

                            with cont1:
                                c1col1, c1col2, c1col3 = st.columns([1.9,1.2,1.9])
                                with c1col1:
                                    st.components.v1.html(inv_logo)
                                with c1col2:
                                    st.image("GFR-logo.png", width=150)
                                    st.markdown(f"<h4 style=''>Invoice Receipt</h4>", unsafe_allow_html=True)
                                with c1col3:
                                    st.write("")
                                    st.write("")
                                    st.markdown("<p style='font-weight: bold; text-align: right;'>Global Fashion Retails Store</p>", unsafe_allow_html=True)
                                    st.markdown(f"<p style='text-align: right;'>Store ID: {inv_storeID}</p>", unsafe_allow_html=True)
                                    st.markdown(f"<p style='text-align: right;'>{inv_city}</p>", unsafe_allow_html=True)
                                    st.markdown(f"<p style='text-align: right;'>{inv_country}</p>", unsafe_allow_html=True)
                                st.divider()
                            
                            with cont2:
                                c2col1, c2col2, c2col3 = st.columns([1,2,1])
                                with c2col1:
                                    st.write("**Bill To:**")
                                    st.write(f"Customer ID: {inv_cID}")
                                    st.write("**Transaction Type:**")
                                    st.write(inv_Ttype)
                                    st.write("**Employee ID:**")
                                    st.write(inv_empID)
                                with c2col3:
                                    st.write("**Invoice #:**")
                                    st.write(inv_ID)
                                    st.write("**Date:**")
                                    st.write(inv_date)
                                    st.write("**Time:**")
                                    st.write(inv_time)
                                st.divider()

                            with cont3:
                               c3start, c3col1, c3col2, c3col3, c3col4 = st.columns([0.25,1,1,1,1])
                               c3col1.write("**Total Quantity**")
                               c3col1.write(inv_quantity)

                               c3col2.write("**Currency**")
                               c3col2.write(inv_currency)

                               c3col3.write("**Payment Method**")
                               c3col3.write(inv_Pmethod)

                               c3col4.write("**Amount**")
                               c3col4.write(f"{inv_CSymbol}{inv_total}")
                               st.divider()

                            with cont4:
                                c4start, c4col1, c4col2 = st.columns([0.25,2,1])
                                c4col1.write("**Note:**")
                                c4col1.write("The Currency of the Net Amount is Converted to USD for Analysis Purpose.")

                                c4col2.write("**Total Amount in USD**")
                                c4col2.markdown(f"<h3 style='font-weight: bold;'>${inv_totalUSD}</h3>", unsafe_allow_html=True)
                                st.divider()

                            with cont5:
                                st.markdown(f"<h5 style='text-align: center;font-family: cursive; font-style: italic'>Thank You for Choosing Global Fashion Retails. Your Style Inspiration Starts Here!</h5>", unsafe_allow_html=True)
                                st.write("")
                                st.write("")
                        
                            

    

    

            

            
            
        
