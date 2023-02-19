import os
import streamlit as st
import numpy as np
from PIL import Image

# Custom imports
from multipage import MultiPage
from pages import trending, eth_vs_sol, definitions, collections # import your pages here

# Create an instance of the app
app = MultiPage()

# Title of the main page
display = Image.open('Logo.jpg')
display = np.array(display)
# st.image(display, width = 400)
# st.title("AnonLabs Data")
col1, col2 = st.columns(2)
col1.image(display, width=100)
col2.title("AnonLabs Data")

# Add all your application here
app.add_page("Trending Collections", trending.app)
app.add_page("Collection Stats", collections.app)
app.add_page("Metric Definitions", definitions.app)
app.add_page("Eth vs. Sol NFT Stats", eth_vs_sol.app)

# The main app
app.run()
