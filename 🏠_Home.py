import streamlit as st 
import pages as pg

# Home page configurations
st.set_page_config(page_title="Homepage", layout="wide", initial_sidebar_state="collapsed")

# Header of the page
with st.container():
    header1, header2 = st.columns([0.8,0.1])
    st.write("")
    st.write("")

# Logout button
with header2:
    st.html("""
        <style>
            .logout{
                border: 1px solid #D3D3D3;
                border-radius: 10px;
                color: black;
                padding: .6em .8em .6em .8em;
                text-decoration: none;
            }
            .logout:hover{
                border: 1px solid #f63366;
                color: #f63366;
            }
        </style>
        <a class="logout" href="https://globalfashionretails.azurewebsites.net/.auth/logout" rel="nofollow noopener">Logout</a>
    """)
    

# Section 1
with st.container():
    homecolstart, homecol1, homecol2 = st.columns([0.2,2,1.5], vertical_alignment='center')
    st.divider()

with homecol1:
    imgcol1, imgcol2 = st.columns([0.3,0.7], border=False)
    imgcol2.image("GFR-logo.png", width=200)
    st.title("Global Fashion Retails Analytics Platform")
    st.markdown("<h5>Transforming Retail Data Into Actionable Business Intelligence</h5>", unsafe_allow_html=True)
    
with homecol2:
    girlpng = """
        <div style="display: flex; justify-content: center; width: 100%; ">
            <iframe src="https://lottie.host/embed/e4d51f71-30ca-4b42-ae78-20e929661a4a/Rcv2Xvq10X.lottie" style="border: None; height: 400px; width: 100%"></iframe>
        </div>
    """
    st.components.v1.html(girlpng, height=450)


# Section 2
with st.container(border=False):
    cont1col1, cont1col2 = st.columns([1.5,2],border=False, vertical_alignment='center')
    st.divider()

with cont1col1:
    cont1png = """
        <div style="display: flex; justify-content: center; width: 100%; ">
            <iframe src="https://lottie.host/embed/775a364b-d740-4c33-a585-746605e3449c/xAChvsM6q1.lottie" style="border: None; height: 400px; width: 100%"></iframe>
        </div>
    """
    st.components.v1.html(cont1png, height=450)

with cont1col2:
    st.markdown("<h4><span style='font-size:50px; font-weight: normal;'>A</span>nalyzing 6M+ transactions, 1.6M+ customers, 1.7k+ products and 35 global stores to drive data-driven decisions and maximize profitability.</h4>", unsafe_allow_html=True)


# Section 3
with st.container():
    cont2colstart, cont2col1, cont2col2 = st.columns([0.2,2,1.5],border=False, vertical_alignment='center')
    st.divider()

with cont2col1:
    st.markdown("<h4><span style='font-size:50px; font-weight: normal;'>T</span>ransform your retail data into actionable insights with comprehensive dashboards, customer profiling, store and product analytics, and interactive visualizations that drive smarter business decisions.</h4>", unsafe_allow_html=True)

with cont2col2:
    cont2png = """
        <div style="display: flex; justify-content: center; width: 100%; ">
            <iframe src="https://lottie.host/embed/b5acc8fd-4fb7-4ff4-8ebd-5b741cadfee7/xhSGnMhEca.lottie" style="border: None; height: 400px; width: 100%"></iframe>
        </div>
    """
    st.components.v1.html(cont2png, height=450)


# Section 4
with st.container():
    cont3col1, cont3col2 = st.columns([1.5,2],border=False, vertical_alignment='center')
    st.divider()

with cont3col1:
    cont3png = """
        <div style="display: flex; justify-content: center; width: 100%; ">
            <iframe src="https://lottie.host/embed/918d02bd-2ac0-46e0-9cf2-7fca943f2bac/6fsDaPgFum.lottie" style="border: None; height: 400px; width: 100%"></iframe>
        </div>
    """
    st.components.v1.html(cont3png, height=450)

with cont3col2:
    st.markdown("<h4><span style='font-size:50px; font-weight: normal;'>B</span>uilt on Microsoft Azure cloud infrastructure with Databricks for data processing. Processing and Analysing Data with enterprise-level security.</h4>", unsafe_allow_html=True)

# Footer of the page
footer = st.container()
with footer:
    st.markdown(
        """
        <h5 style='text-align: center; font-style: italic; font-family: cursive'>Platform Created by Yasser Mohammed</h5>
    """, unsafe_allow_html=True )

