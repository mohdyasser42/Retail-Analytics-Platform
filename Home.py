import streamlit as st 
import pandas as pd
from config import read_parquet_from_adls
import io
import pages as pg


st.set_page_config(page_title="Dashboard", layout="wide")



homecol1, homecol2 = st.columns([2,1.5],border=False)

with homecol1:
    st.image("GFR-logo.png", width=200)
    st.title("Global Fashion Retails")
    st.write("Get Latest Fashion Trends For Your Family At Best Prices")
    
with homecol2:
    st.link_button("Logout", "https://globalfashionretails.azurewebsites.net/.auth/logout")
    girlpng = """
        <div style="display: flex; justify-content: center; width: 100%; ">
            <iframe src="https://lottie.host/embed/7aa7dc66-04d5-4198-84ed-3b2784cd1bb0/bHEnneQUnr.lottie" style="border: None; height: 400px; width: 600px"></iframe>
        </div>
    """
    st.components.v1.html(girlpng, height=600)
