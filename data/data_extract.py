"""

"""
import pandas as pd
import time
import requests
import json

from utils import helper_functions
from loguru import logger
from shroomdk import ShroomDK

logger.debug("Starting up logger...")

# Initialize `ShroomDK` with your API Key
sdk = ShroomDK("5b604b69-974d-4136-bbfc-7f6e1b2986db")


def make_contract_request(sort_by: str, url: str, api_key: str) -> json:
    """

    :return:
    """
    if not sort_by:
        sort_by = 'SALES'

    query_start = 'query TrendingCollections($first: Int) {'

    sorting_piece = f'contracts(orderBy: {sort_by}, orderDirection: DESC, first: $first)'

    query_body = """{
      edges {
        node {
          address
          ... on ERC721Contract {
            name
            stats {
              totalSales
              average
              ceiling
              floor
              volume
            }
            symbol
          }
        }
      }
      pageInfo {
        hasNextPage
        hasPreviousPage
        startCursor
        endCursor
      }
    }
  }"""

    full_query = query_start+sorting_piece+query_body
    headers = {'x-api-key': api_key}

    _json = {'query': full_query, 'variables': {'first': 50}}

    r = requests.post(url=url, json=_json, headers=headers)

    dat = r.json()

    response = [v for i in dat['data']['contracts']['edges'] for k, v in i.items() if i['node']['name'] != '']

    return [helper_functions.flatten(_dict) for _dict in response]


############################################3


def query_to_df(query: str) -> pd.DataFrame:
    """
    Run the query against Flipside's query engine and await the results

    :param query:
    :return:
    """
    query_result_set = sdk.query(query)

    try:

        df = pd.DataFrame(query_result_set.records)

        run_stats = query_result_set.run_stats

        logger.info(
            "Query ran in "
            + str(run_stats.ended_at - run_stats.started_at)
            + f" and returned {run_stats.record_count} records."
        )

        return df

    except KeyError:
        logger.info(query_result_set.error)


def get_sol_mktplace_stats(months, date_trunc) -> pd.DataFrame:
    query = f"""
    with

sol_price as (

  select

  	date_trunc('hour',block_timestamp) as date_hour,

  	case
  		when swap_to_mint = 'So11111111111111111111111111111111111111112' then swap_to_mint
  		else swap_from_mint
  	end as token_address,

  	sum(case when swap_to_mint = 'So11111111111111111111111111111111111111112' then swap_from_amount else swap_to_amount end) as stable_amount,
  	sum(case when swap_to_mint = 'So11111111111111111111111111111111111111112' then swap_to_amount else swap_from_amount end) as token_amount,

  	stable_amount / token_amount as token_price

  from solana.core.fact_swaps swaps
  where succeeded = TRUE
  	and block_timestamp >= '2022-01-01'
  	and (swap_from_mint = 'So11111111111111111111111111111111111111112'
  		or swap_to_mint = 'So11111111111111111111111111111111111111112')
  	and (swap_from_mint in ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v','Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB')
  		or swap_to_mint in ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v','Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'))
  group by 1,2
),

me_volume as (

  select

  	date_trunc('{date_trunc}',block_timestamp) as date,
  	case when marketplace = 'solana monkey business marketplace' then 'SMB Market'
  		else marketplace end as marketplace,
  	sum(sales_amount) as volume_sol,
  	sum(sales_amount * token_price) as volume_usd,
  	count(1) as trades

  from solana.core.fact_nft_sales sales 
  left join sol_price
  	on date_trunc('hour',sales.block_timestamp) = sol_price.date_hour
  where block_timestamp >= current_date() - interval '{months} months'
  	and sales_amount * token_price < 1e7
  	and succeeded
  group by 1,2
)

select * from me_volume
order by marketplace
    """
    t_1 = time.time()
    logger.info("Executing query...")

    try:
        df = query_to_df(query)

        t_2 = time.time()

        logger.info(f"Query was executed in: {t_2 - t_1}")

        return df

    except KeyError:
        logger.error("Query failed to execute, please confirm syntax")


def get_eth_mktplace_stats(months, date_trunc) -> pd.DataFrame:
    query = f"""
    with

eth_volume as (

  select

  	date_trunc('{date_trunc}',block_timestamp) as date,
  	platform_name as marketplace,
  	sum(price_usd) as volume_usd,
  	count(1) as trades

  from ethereum.core.ez_nft_sales
  where block_timestamp >= current_date() - interval '{months} months'
  	and price_usd < 1e7
  group by 1,2
)

select * from eth_volume
order by marketplace
    """
    t_1 = time.time()
    logger.info("Executing query...")

    try:
        df = query_to_df(query)

        t_2 = time.time()

        logger.info(f"Query was executed in: {t_2 - t_1}")

        return df

    except KeyError:
        logger.error("Query failed to execute, please confirm syntax")


def get_distinct_buyers_vs_sellers(months: int) -> pd.DataFrame:
    query = f"""
            with
    
    sol_volume as (
    
      select
    
        'Solana' as blockchain,
        purchaser as buyer_address,
        nth_value(f.value,2) over (partition by tx_id order by f.this:lamports::int desc)::string as seller_address
    
      from solana.core.fact_nft_sales sales 
      inner join solana.core.fact_transactions
        using(tx_id)
      inner join lateral flatten(input => inner_instructions, recursive => TRUE) f
      where sales.block_timestamp >= current_date() - interval '{months} months'
        and sales.succeeded
        and f.key = 'destination'
        and f.this:lamports::int > 0
      qualify row_number() over (partition by tx_id order by f.this:lamports::int desc) = 1
    ),
    
    eth_volume as (
    
      select
    
        'Ethereum' as blockchain,
        buyer_address,
        seller_address
    
      from ethereum.core.ez_nft_sales
      where block_timestamp >= current_date() - interval '{months} months'
        and price_usd < 1e7
    )
    
    select
    
        blockchain,
        count(distinct buyer_address) as buyers,
        count(distinct seller_address) as sellers
    
    from (select * from sol_volume union all select * from eth_volume)
    group by 1
    order by blockchain
    """
    t_1 = time.time()
    logger.info("Executing query...")

    try:
        df = query_to_df(query)

        t_2 = time.time()

        logger.info(f"Query was executed in: {t_2 - t_1}")

        return df

    except KeyError:
        logger.error("Query failed to execute, please confirm syntax")


def get_eth_vs_sol_nft_stats(months, date_trunc) -> pd.DataFrame:
    """

    :return:
    """
    query = f"""
        with
            sol_price as (

              select

                date_trunc('hour',block_timestamp) as date_hour,

                case
                    when swap_to_mint = 'So11111111111111111111111111111111111111112'
                        then swap_to_mint
                    else swap_from_mint
                end as token_address,

                sum(case when swap_to_mint = 'So11111111111111111111111111111111111111112'
                    then swap_from_amount else swap_to_amount end) as stable_amount,
                sum(case when swap_to_mint = 'So11111111111111111111111111111111111111112'
                    then swap_to_amount else swap_from_amount end) as token_amount,

                stable_amount / token_amount as token_price

              from solana.core.fact_swaps swaps
              where succeeded = TRUE
                and block_timestamp >= current_date() - interval '{months} months'
                and (swap_from_mint = 'So11111111111111111111111111111111111111112'
                    or swap_to_mint = 'So11111111111111111111111111111111111111112')
                and (swap_from_mint in
                    ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
                    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB')
                    or swap_to_mint in
                        ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
                        'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'))
              group by 1,2
            ),

            sol_volume as (

              select

                block_timestamp,
                'Solana' as blockchain,
                purchaser as buyer_address,
                nth_value(f.value,2) over
                    (partition by tx_id order by f.this:lamports::int desc)::string as seller_address,
                sales_amount * token_price as price_usd

              from solana.core.fact_nft_sales sales
              left join sol_price
                on date_trunc('hour',sales.block_timestamp) = sol_price.date_hour
              inner join solana.core.fact_transactions
                using(block_timestamp,tx_id)
              inner join lateral flatten(input => inner_instructions, recursive => TRUE) f
              where sales.block_timestamp >= current_date() - interval '{months} months'
                and sales_amount * token_price < 1e7
                and sales_amount * token_price >= 1
                and sales.succeeded
                and f.key = 'destination'
                and f.this:lamports::int > 0
              qualify row_number() over
              (partition by tx_id order by f.this:lamports::int desc) = 1
            ),

            eth_volume as (

              select

                block_timestamp,
                'Ethereum' as blockchain,
                buyer_address,
                seller_address,
                price_usd

              from ethereum.core.ez_nft_sales
              where block_timestamp >= current_date() - interval '{months} months'
                and price_usd < 1e7
                and price_usd >= 1
            )

        select

            date_trunc('{date_trunc}',block_timestamp) as date,
            blockchain,
            count(1) as trades,
            count(distinct buyer_address) as buyers,
            count(distinct seller_address) as sellers,
            sum(price_usd) as volume_usd

    from (select * from sol_volume union all select * from eth_volume)
    group by 1,2
    order by 1,2 ASC
    """
    t_1 = time.time()
    logger.info("Executing query...")

    try:
        df = query_to_df(query)

        t_2 = time.time()

        logger.info(f"Query was executed in: {t_2 - t_1}")

        return df

    except KeyError:
        logger.error("Query failed to execute, please confirm syntax")
