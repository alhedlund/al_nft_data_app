import pandas as pd
import streamlit as st
import os

from dotenv import load_dotenv
from data import data_extract

load_dotenv()

TRENDING_URL = os.getenv('URL')
API_KEY = os.getenv('API_KEY')


def app():
    original_title = '<p style="font-family:Monospace; color:White; font-size: 24px;">Trending NFT Collections</p>'
    st.markdown(original_title, unsafe_allow_html=True)

    trending_sort_choices = ['SALES', 'VOLUME', 'FLOOR', 'AVERAGE']
    sort_by = st.sidebar.selectbox("Trending By:", trending_sort_choices)

    event_type = st.sidebar.selectbox(
            "Trending Period",
            ['15min', '30min', '1hr', '24hr']
        )

    trending = data_extract.make_contract_request(sort_by, TRENDING_URL, API_KEY)

    df = pd.DataFrame(trending)

    df.rename(columns={
        'address': 'Address',
        'name': 'Name',
        'stats_totalSales': 'Total Sales',
        'stats_average': 'Average',
        'stats_ceiling': 'Ceiling',
        'stats_floor': 'Floor',
        'stats_volume': 'Volume'
    }, inplace=True)

    df.drop(columns=['symbol'], inplace=True)

    df = df[[
        'Name', 'Total Sales', 'Average',
        'Ceiling', 'Floor', 'Volume', 'Address'
    ]]

    # CSS to inject contained in a string
    hide_dataframe_row_index = """
                <style>
                .row_heading.level0 {display:none}
                .blank {display:none}
                </style>
                """

    # Inject CSS with Markdown
    st.markdown(hide_dataframe_row_index, unsafe_allow_html=True)

    st.dataframe(df)

    @st.cache
    def convert_df(base_df):
        # IMPORTANT: Cache the conversion to prevent computation on every rerun
        return base_df.to_csv().encode('utf-8')

    csv = convert_df(df)

    st.download_button(
        label="Download data as CSV",
        data=csv,
        file_name='trending.csv',
        mime='text/csv',
    )
