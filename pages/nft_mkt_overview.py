import pandas as pd
import streamlit as st
import data.data_extract as de
import plotly.express as px


def app():
    original_title = '<p style="font-family:Monospace; color:White; font-size: 24px;">NFT Market Stats</p>'
    st.markdown(original_title, unsafe_allow_html=True)

    # Generate the inputs

    time_period_choice = st.sidebar.number_input(
        "Time Period Days",
        min_value=1,
        max_value=180,
        value=180,
        step=30
    )

    marketplace_choice = st.sidebar.text_input(label="Marketplace", value="All")

    contract_address_choice = st.sidebar.text_input(label="Contract Address", value="-")

    wash_trade_filter = st.sidebar.text_input(label="WashTradeFilter", value="On")

    currency_selector = st.sidebar.selectbox(label="Currency", options=["USD", "ETH"])

    try:
        #################
        # Get volume data

        daily_nft_df = de.get_daily_nft_volume(
            TimePeriodDays=time_period_choice,
            Marketplace=marketplace_choice,
            ContractAddress=contract_address_choice,
            WashTradeFilter=wash_trade_filter,
            Currency=currency_selector
        )

        quick_vol_stat_df = de.get_nft_trading_volume(
            TimePeriodDays=time_period_choice,
            Marketplace=marketplace_choice,
            ContractAddress=contract_address_choice,
            WashTradeFilter=wash_trade_filter,
            Currency=currency_selector
        )

        if currency_selector == "ETH":
            vol_col_name = "trading_volume__eth"
        else:
            vol_col_name = "trading_volume__usd"

        vol_only_daily_df = daily_nft_df[["utc_date", vol_col_name, "period"]]

        st.write("Daily Trading Volume")
        fig = px.line(vol_only_daily_df, x="utc_date", y=vol_col_name, height=400)

        fig.update_layout(yaxis_title=None, xaxis_title=None)

        st.plotly_chart(fig, theme='streamlit', use_container_width=True)

        st.write("NFT Trading Volume")
        st.dataframe(quick_vol_stat_df)

        ################
        # Get the trading wallet data

        quick_wallet_stat_df = de.get_uniq_trdng_wlt_stats(
            TimePeriodDays=time_period_choice,
            Marketplace=marketplace_choice,
            ContractAddress=contract_address_choice,
            WashTradeFilter=wash_trade_filter,
            Currency=currency_selector
        )

        wallet_only_daily_df = daily_nft_df[["utc_date", "total_trading_wallets", "period"]]

        st.write("Daily Trading Wallets")
        fig_2 = px.line(wallet_only_daily_df, x="utc_date", y="total_trading_wallets",
                        height=400)

        fig_2.update_layout(yaxis_title=None, xaxis_title=None)

        st.plotly_chart(fig_2, theme='streamlit', use_container_width=True)

        st.write("Unique Trading Wallets")
        st.dataframe(quick_wallet_stat_df)

        ################
        # Get the volume per wallet data
        vol_per_wallet_stat_df = de.get_vol_per_wlt_stats(
            TimePeriodDays=time_period_choice,
            Marketplace=marketplace_choice,
            ContractAddress=contract_address_choice,
            WashTradeFilter=wash_trade_filter,
            Currency=currency_selector
        )

        if currency_selector == "ETH":
            vol_wallet_col_name = "volume_per_wallet__eth"
        else:
            vol_wallet_col_name = "volume_per_wallet__usd"

        avg_wallet_vol_daily_df = daily_nft_df[["utc_date", vol_wallet_col_name, "period"]]

        st.write("Daily Avg. Volume Per Wallet")
        fig_3 = px.line(avg_wallet_vol_daily_df, x="utc_date", y=vol_wallet_col_name,
                      height=400)

        st.plotly_chart(fig_3, theme='streamlit', use_container_width=True)

        st.write("Volume per Wallet")
        st.dataframe(vol_per_wallet_stat_df)

        ################
        # Get the txn per wallet data

        txn_per_wlt_stat_df = de.get_txns_per_wlt_stats(
            TimePeriodDays=time_period_choice,
            Marketplace=marketplace_choice,
            ContractAddress=contract_address_choice,
            WashTradeFilter=wash_trade_filter,
            Currency=currency_selector
        )

        txns_per_wallet_daily_df = daily_nft_df[["utc_date", "transactions_per_wallet", "period"]]

        st.write("Daily Avg. Transactions Per Wallet")
        fig_4 = px.line(txns_per_wallet_daily_df, x="utc_date", y="transactions_per_wallet",
                        height=400)

        st.plotly_chart(fig_4, theme='streamlit', use_container_width=True)

        st.write("Transactions per Wallet")
        st.dataframe(txn_per_wlt_stat_df)

        ################
        # Get the daily avg txn amnt data

        txn_fee_stat_df = de.get_vol_per_txn_stats(
            TimePeriodDays=time_period_choice,
            Marketplace=marketplace_choice,
            ContractAddress=contract_address_choice,
            WashTradeFilter=wash_trade_filter,
            Currency=currency_selector
        )

        if currency_selector == "ETH":
            avg_txn_amt_col_name = "volume_per_tx__eth"
        else:
            avg_txn_amt_col_name = "volume_per_tx__usd"

        daily_avg_txn_amt_df = daily_nft_df[["utc_date", avg_txn_amt_col_name, "period"]]

        st.write("Daily Avg. Transaction Amount")
        fig_5 = px.line(daily_avg_txn_amt_df, x="utc_date", y=avg_txn_amt_col_name,
                        height=400)

        st.plotly_chart(fig_5, theme='streamlit', use_container_width=True)

        st.write("Volume per Transaction")
        st.dataframe(txn_fee_stat_df)

        ################
        # Get the daily creator fee data

        creator_fee_stat_df = de.get_creator_fees_stats(
            TimePeriodDays=time_period_choice,
            Marketplace=marketplace_choice,
            ContractAddress=contract_address_choice,
            WashTradeFilter=wash_trade_filter,
            Currency=currency_selector
        )

        if currency_selector == "ETH":
            creator_fee_col_name = "creator_fees__eth"
        else:
            creator_fee_col_name = "creator_fees__usd"

        daily_creator_fee_df = daily_nft_df[["utc_date", creator_fee_col_name, "period"]]

        st.write("Daily Creator Fees")
        fig_6 = px.line(daily_creator_fee_df, x="utc_date", y=creator_fee_col_name,
                        height=400)

        st.plotly_chart(fig_6, theme='streamlit', use_container_width=True)

        st.write("Creator Fees")
        st.dataframe(creator_fee_stat_df)

        ################
        # Get the daily platform fee data

        plat_fee_stat_df = de.get_platform_fees_stats(
            TimePeriodDays=time_period_choice,
            Marketplace=marketplace_choice,
            ContractAddress=contract_address_choice,
            WashTradeFilter=wash_trade_filter,
            Currency=currency_selector
        )

        if currency_selector == "ETH":
            plat_fee_col_name = "platform_fees__eth"
        else:
            plat_fee_col_name = "platform_fees__usd"

        daily_platform_fee_df = daily_nft_df[["utc_date", plat_fee_col_name, "period"]]

        st.write("Daily Platform Fees")
        fig_7 = px.line(daily_platform_fee_df, x="utc_date", y=plat_fee_col_name,
                        height=400)

        st.plotly_chart(fig_7, theme='streamlit', use_container_width=True)

        st.write("Platform Fees")
        st.dataframe(plat_fee_stat_df)

    except Exception as e:
        st.write(f"Request failed with exception: {e}.")


