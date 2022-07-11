"""
"""
import datetime as dt
import requests
import time
import pandas as pd
import streamlit as st


def app():
    asset_contract_address = st.sidebar.text_input("Contract Address")

    start_dt_input = st.sidebar.date_input(label='Start Date')
    end_dt_input = st.sidebar.date_input(label='End Date')

    def get_historical_collection_stats(address: str,
                                        start_dt: dt.datetime,
                                        end_dt: dt.datetime
                                        ) -> pd.DataFrame:
        """

        """

        # turn start and end dates into unix timestamps
        import time

        start_ts = time.mktime(start_dt.timetuple())
        end_ts = time.mktime(end_dt.timetuple())

        url = "https://api.reservoir.tools/collections/daily-volumes/v1"

        headers = {
            "Accept": "*/*",
            "x-api-key": "demo-api-key"
        }

        params = {
            'id': address,
            'startTimestamp': start_ts,
            'endTimestamp': end_ts
        }

        response = requests.get(url, headers=headers, params=params)

        resp_df = pd.DataFrame(response.json()['collections'])

        resp_df['timestamp'] = pd.to_datetime(resp_df['timestamp'], unit='s')

        resp_df = resp_df.set_index(pd.DatetimeIndex(resp_df['timestamp']))

        return resp_df

    try:
        df = get_historical_collection_stats(asset_contract_address, start_dt_input, end_dt_input)

        st.write('Average Floor Price')
        st.line_chart(df['floor_sell_value'])

        st.write('Volume')
        st.line_chart(df['volume'])

        st.write('Sales Count')
        st.line_chart(df['sales_count'])

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

    except KeyError:

        error_text = """
        No Data for the select asset and date combination.
        
        Please change parameters.
        """

        st.write(error_text)
