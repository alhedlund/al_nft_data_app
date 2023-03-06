import pandas as pd
import streamlit as st
import data.mktplace_data_extract as mde
import plotly.express as px

from utils.helper_functions import add_pct_change_cols, add_avg_trade_size_by_mktplace_cols, calc_cat_share_by_metric


def app():
    original_title = '<p style="font-family:Monospace; color:White; font-size: 24px;">Ethereum & Solana: NFT Stats by Marketplace</p>'
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
        eth_df = mde.get_eth_mktplace_stats(date_trunc=date_trunc, months=month_choice)
        sol_df = mde.get_sol_mktplace_stats(date_trunc=date_trunc, months=month_choice)

        # transform df
        trans_eth_df = (eth_df
                        .pipe(add_avg_trade_size_by_mktplace_cols)
                        .pipe(add_pct_change_cols, 'marketplace')
                        )

        trans_sol_df = (sol_df
                        .pipe(add_avg_trade_size_by_mktplace_cols)
                        .pipe(add_pct_change_cols, 'marketplace')
                        )

        #############
        st.write("ETH NFT Volume (USD) by Marketplace")
        fig = px.bar(trans_eth_df, x="date", y="volume_usd",
                     color='marketplace',
                     height=400)

        fig.update_layout(yaxis_title=None, xaxis_title=None)

        st.plotly_chart(fig, theme='streamlit', use_container_width=True)

        #############
        # st.write("ETH NFT Volume (USD) Share by Marketplace")
        #
        # trans_eth_share_df = (trans_eth_df
        #                       .pipe(calc_cat_share_by_metric, cat_col='marketplace', metric_col='volume_usd')
        #                       .reset_index())
        #
        # fig = px.area(trans_eth_share_df, x="date", y="volume_usd",
        #               color='marketplace',
        #               height=400
        #               )
        #
        # fig.update_layout(yaxis_title=None, xaxis_title=None)
        #
        # st.plotly_chart(fig, theme='streamlit', use_container_width=True)

        #############
        st.write("ETH NFT Volume (USD) W/W by Marketplace")
        fig_2 = px.histogram(trans_eth_df, x="date", y="volume_usd W/W",
                             color='marketplace',
                             barmode='group',
                             height=400)

        fig_2.update_layout(yaxis_title=None, xaxis_title=None, yaxis_tickformat="2%")

        st.plotly_chart(fig_2, theme='streamlit', use_container_width=True)

        #############
        st.write("ETH NFT Trades by Marketplace")
        fig = px.bar(trans_eth_df, x="date", y="trades",
                     color='marketplace',
                     height=400)

        fig.update_layout(yaxis_title=None, xaxis_title=None)

        st.plotly_chart(fig, theme='streamlit', use_container_width=True)

        #############
        st.write("ETH NFT Trades W/W by Marketplace")
        fig_2 = px.histogram(trans_eth_df, x="date", y="trades W/W",
                             color='marketplace', barmode='group',
                             height=400)

        fig_2.update_layout(yaxis_title=None, xaxis_title=None, yaxis_tickformat="2%")

        st.plotly_chart(fig_2, theme='streamlit', use_container_width=True)

        #############
        st.write("ETH NFT Avg. Trade Size (USD) by Marketplace")
        fig = px.bar(trans_eth_df, x="date", y="avg. trade size by marketplace",
                     color='marketplace',
                     height=400)

        fig.update_layout(yaxis_title=None, xaxis_title=None)

        st.plotly_chart(fig, theme='streamlit', use_container_width=True)

        #############
        st.write("ETH NFT Avg. Trade Size (USD) W/W by Marketplace")
        fig_2 = px.histogram(trans_eth_df, x="date", y="avg. trade size by marketplace W/W",
                             color='marketplace', barmode='group',
                             height=400)

        fig_2.update_layout(yaxis_title=None, xaxis_title=None, yaxis_tickformat="2%")

        st.plotly_chart(fig_2, theme='streamlit', use_container_width=True)

        #############
        st.write("ETH NFT Platform Fees (USD) by Marketplace")
        fig = px.bar(trans_eth_df, x="date", y="platform_fee_usd",
                           color='marketplace',
                           height=400)

        fig.update_layout(yaxis_title=None, xaxis_title=None)

        st.plotly_chart(fig, theme='streamlit', use_container_width=True)

        #############
        st.write("ETH NFT Platform Fees (USD) W/W by Marketplace")
        fig_2 = px.histogram(trans_eth_df, x="date", y="platform_fee_usd W/W",
                             color='marketplace', barmode='group',
                             height=400)

        fig_2.update_layout(yaxis_title=None, xaxis_title=None, yaxis_tickformat="2%")

        st.plotly_chart(fig_2, theme='streamlit', use_container_width=True)

        #############
        st.write("ETH NFT Creator Fees (USD) by Marketplace")
        fig = px.bar(trans_eth_df, x="date", y="creator_fee_usd",
                           color='marketplace',
                           height=400)

        fig.update_layout(yaxis_title=None, xaxis_title=None)

        st.plotly_chart(fig, theme='streamlit', use_container_width=True)

        #############
        st.write("ETH NFT Creator Fees (USD) W/W by Marketplace")
        fig_2 = px.histogram(trans_eth_df, x="date", y="creator_fee_usd W/W",
                             color='marketplace', barmode='group',
                             height=400)

        fig_2.update_layout(yaxis_title=None, xaxis_title=None, yaxis_tickformat="2%")

        st.plotly_chart(fig_2, theme='streamlit', use_container_width=True)

        #############
        st.write("SOL NFT Volume (USD) by Marketplace")
        fig_3 = px.bar(trans_sol_df, x="date", y="volume_usd",
                           color='marketplace',
                           height=400)

        fig_3.update_layout(yaxis_title=None, xaxis_title=None)

        st.plotly_chart(fig_3, theme='streamlit', use_container_width=True)

        #############
        st.write("SOL NFT Volume (USD) W/W by Marketplace")
        fig_4 = px.histogram(trans_sol_df, x="date", y="volume_usd W/W",
                             color='marketplace', barmode='group',
                             height=400)

        fig_4.update_layout(yaxis_title=None, xaxis_title=None, yaxis_tickformat="2%")

        st.plotly_chart(fig_4, theme='streamlit', use_container_width=True)

        #############
        st.write("SOL NFT Trades by Marketplace")
        fig = px.bar(trans_sol_df, x="date", y="trades",
                     color='marketplace',
                     height=400)

        fig.update_layout(yaxis_title=None, xaxis_title=None)

        st.plotly_chart(fig, theme='streamlit', use_container_width=True)

        #############
        st.write("SOL NFT Trades W/W by Marketplace")
        fig_2 = px.histogram(trans_sol_df, x="date", y="trades W/W",
                             color='marketplace', barmode='group',
                             height=400)

        fig_2.update_layout(yaxis_title=None, xaxis_title=None, yaxis_tickformat="2%")

        st.plotly_chart(fig_2, theme='streamlit', use_container_width=True)

        #############
        st.write("SOL NFT Avg. Trade Size (USD) by Marketplace")
        fig = px.bar(trans_sol_df, x="date", y="avg. trade size by marketplace",
                     color='marketplace',
                     height=400)

        fig.update_layout(yaxis_title=None, xaxis_title=None)

        st.plotly_chart(fig, theme='streamlit', use_container_width=True)

        #############
        st.write("SOL NFT Avg. Trade Size (USD) W/W by Marketplace")
        fig_2 = px.histogram(trans_sol_df, x="date", y="avg. trade size by marketplace W/W",
                             color='marketplace', barmode='group',
                             height=400)

        fig_2.update_layout(yaxis_title=None, xaxis_title=None, yaxis_tickformat="2%")

        st.plotly_chart(fig_2, theme='streamlit', use_container_width=True)

        #############
        # CSS to inject contained in a string
        hide_dataframe_row_index = """
                        <style>
                        .row_heading.level0 {display:none}
                        .blank {display:none}
                        </style>
                        """

        # Inject CSS with Markdown
        st.markdown(hide_dataframe_row_index, unsafe_allow_html=True)

        @st.cache
        def convert_df(base_df):
            # IMPORTANT: Cache the conversion to prevent computation on every rerun
            return base_df.to_csv().encode('utf-8')

        #############

        st.write("ETH Stats by Marketplace")
        st.dataframe(trans_eth_df)

        eth_csv = convert_df(trans_eth_df)

        st.download_button(
            label="Download ETH marketplace data as CSV",
            data=eth_csv,
            file_name='trans_eth_df.csv',
            mime='text/csv',
        )

        #############

        st.write("SOL Stats by Marketplace")
        st.dataframe(trans_sol_df)

        sol_csv = convert_df(trans_sol_df)

        st.download_button(
            label="Download SOL marketplace data as CSV",
            data=sol_csv,
            file_name='trans_sol_df.csv',
            mime='text/csv',
        )

    except Exception as e:
        st.write(f"Request failed with exception: {e}.")

