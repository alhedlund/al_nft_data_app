import pandas as pd
import streamlit as st
import data.data_extract as de
import plotly.express as px

from utils.helper_functions import add_pct_change_cols


def app():
    original_title = '<p style="font-family:Monospace; color:White; font-size: 24px;">Ethereum vs Solana: NFT Trading Activity</p>'
    st.markdown(original_title, unsafe_allow_html=True)

    date_trunc_choices = ['week', 'month']
    date_trunc = st.sidebar.selectbox("date_trunc", date_trunc_choices)

    month_choice = st.sidebar.number_input(
        "months",
        min_value=1,
        max_value=12,
        value=3,
        step=1
    )

    try:
        full_df = de.get_eth_vs_sol_nft_stats(date_trunc=date_trunc, months=month_choice)

        # transform df
        trans_df = add_pct_change_cols(full_df, "blockchain")

        # distinct buyers vs. sellers df
        # buy_sell_df = de.get_distinct_buyers_vs_sellers(months=month_choice)

        cumul_stats_df = de.get_cumltv_nft_stats(months=month_choice)

        #############
        # Header section with cumulative eth vs. sol stats

        with st.container():
            col1, col2 = st.columns(2)
            with col1:
                cum_eth_df = cumul_stats_df[cumul_stats_df['blockchain'] == 'Ethereum']
                st.write(f'Ethereum Cumulative Trades: {cum_eth_df["trades"].values[0]:,.2f}')
            with col2:
                st.write(f'Ethereum Cumulative Volume (USD): ${cum_eth_df["volume_usd"].values[0]:,.2f}')

        with st.container():
            col1, col2 = st.columns(2)
            with col1:
                cum_sol_df = cumul_stats_df[cumul_stats_df['blockchain'] == 'Solana']
                st.write(f'Solana Cumulative Trades: {cum_sol_df["trades"].values[0]:,.2f}')
            with col2:
                st.write(f'Solana Cumulative Volume (USD): ${cum_sol_df["volume_usd"].values[0]:,.2f}')

        #############
        # Header section with distinct eth vs. sol buy/sell stats
        #
        # with st.container():
        #     col1, col2 = st.columns(2)
        #     with col1:
        #         eth_df = buy_sell_df[buy_sell_df['blockchain'] == 'Ethereum']
        #         st.write(f'Ethereum Distinct Buyers: {eth_df["buyers"].values[0]}')
        #     with col2:
        #         st.write(f'Ethereum Distinct Sellers: {eth_df["sellers"].values[0]}')
        #
        # with st.container():
        #     col1, col2 = st.columns(2)
        #     with col1:
        #         sol_df = buy_sell_df[buy_sell_df['blockchain'] == 'Solana']
        #         st.write(f'Solana Distinct Buyers: {sol_df["buyers"].values[0]}')
        #     with col2:
        #         st.write(f'Solana Distinct Sellers: {sol_df["sellers"].values[0]}')

        #############
        st.write("NFT Buyers")
        fig = px.histogram(trans_df, x="date", y="buyers",
                           color='blockchain', barmode='group',
                           height=400)

        fig.update_layout(yaxis_title=None, xaxis_title=None)

        st.plotly_chart(fig, theme='streamlit', use_container_width=True)

        #############
        st.write("NFT Buyers W/W")
        fig_2 = px.histogram(trans_df, x="date", y="buyers W/W",
                           color='blockchain', barmode='group',
                           height=400)

        fig_2.update_layout(yaxis_title=None, xaxis_title=None, yaxis_tickformat="2%")

        st.plotly_chart(fig_2, theme='streamlit', use_container_width=True)

        #############
        st.write("NFT Sellers")
        fig_3 = px.histogram(trans_df, x="date", y="sellers",
                           color='blockchain', barmode='group',
                           height=400)

        st.plotly_chart(fig_3, theme='streamlit', use_container_width=True)

        #############
        st.write("NFT Sellers W/W")
        fig_4 = px.histogram(trans_df, x="date", y="sellers W/W",
                             color='blockchain', barmode='group',
                             height=400)

        fig_4.update_layout(yaxis_title=None, xaxis_title=None, yaxis_tickformat="2%")

        st.plotly_chart(fig_4, theme='streamlit', use_container_width=True)

        #############
        st.write("NFT Volume")
        fig_5 = px.histogram(trans_df, x="date", y="volume_usd",
                           color='blockchain', barmode='group',
                           height=400)

        st.plotly_chart(fig_5, theme='streamlit', use_container_width=True)

        #############
        st.write("NFT Volume W/W")
        fig_6 = px.histogram(trans_df, x="date", y="volume_usd W/W",
                             color='blockchain', barmode='group',
                             height=400)

        fig_6.update_layout(yaxis_title=None, xaxis_title=None, yaxis_tickformat="2%")

        st.plotly_chart(fig_6, theme='streamlit', use_container_width=True)

        #############
        st.write("NFT Trades")
        fig_7 = px.histogram(trans_df, x="date", y="trades",
                           color='blockchain', barmode='group',
                           height=400)

        st.plotly_chart(fig_7, theme='streamlit', use_container_width=True)

        #############
        st.write("NFT Trades W/W")
        fig_8 = px.histogram(trans_df, x="date", y="trades W/W",
                             color='blockchain', barmode='group',
                             height=400)

        fig_8.update_layout(yaxis_title=None, xaxis_title=None, yaxis_tickformat="2%")

        st.plotly_chart(fig_8, theme='streamlit', use_container_width=True)


        # st.dataframe(trans_df)
        #
        # @st.cache
        # def convert_df(base_df):
        #     # IMPORTANT: Cache the conversion to prevent computation on every rerun
        #     return base_df.to_csv().encode('utf-8')
        #
        # csv = convert_df(trans_df)
        #
        # st.download_button(
        #     label="Download data as CSV",
        #     data=csv,
        #     file_name='eth_vs_sol_stats.csv',
        #     mime='text/csv',
        # )

    except Exception as e:
        st.write(f"Request failed with exception: {e}.")

