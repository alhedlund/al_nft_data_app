import os
import streamlit as st
import numpy as np
from PIL import Image

# Custom imports
from multipage import MultiPage
from pages import trending, eth_vs_sol, definitions, collections, nft_mkt_overview # import your pages here

st.set_page_config(layout="wide")

# Create an instance of the app
app = MultiPage()

# Title of the main page
display = Image.open('Logo.jpg')
display = np.array(display)
# st.image(display, width = 400)
# st.title("AnonLabs Data")
col1, col2 = st.columns(2)
col1.image(display, width=100)
col2.title("AnonLabs Data Platform")

# Add all your application here
app.add_page("NFT Market Overview", nft_mkt_overview.app)
app.add_page("Eth vs. Sol NFT Stats", eth_vs_sol.app)
app.add_page("Trending Collections", trending.app)
app.add_page("Collection Stats", collections.app)
app.add_page("Metric Definitions", definitions.app)

# The main app
app.run()
