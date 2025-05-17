import streamlit as st 
import pandas as pd
from config import read_parquet_from_adls
import io
import pages as pg


st.set_page_config(page_title="Dashboard", layout="wide")



st.title("Global Fashion Retails")


st.write("This is homepage")