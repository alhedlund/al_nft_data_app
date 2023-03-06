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


############################################


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

def get_cumltv_nft_stats(months) -> pd.DataFrame:
    """

    :return:
    """
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
            and block_timestamp >= current_date() - interval '{months} months'
            and (swap_from_mint = 'So11111111111111111111111111111111111111112'
                or swap_to_mint = 'So11111111111111111111111111111111111111112')
            and (swap_from_mint in ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v','Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB')
                or swap_to_mint in ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v','Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'))
          group by 1,2
        ),
    
        sol_volume as (
        
          select
        
            block_timestamp,
            'Solana' as blockchain,
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
          qualify row_number() over (partition by tx_id order by f.this:lamports::int desc) = 1
        ),
    
        eth_volume as (
        
          select
        
            block_timestamp,
            'Ethereum' as blockchain,
            price_usd
        
          from ethereum.core.ez_nft_sales
          where block_timestamp >= current_date() - interval '{months} months'
            and price_usd < 1e7
            and price_usd >= 1
        )
    
    select
    
        blockchain,
        count(1) as trades,
        sum(price_usd) as volume_usd
    
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

######################################

def get_daily_nft_volume(
        TimePeriodDays: int = 365,
        Marketplace: str = "All",
        ContractAddress: str = "",
        WashTradeFilter: str = "On",
        Currency: str = "USD") -> pd.DataFrame:
    """

    :return:
    """
    query = f"""
    WITH
        nft_sales AS (
            SELECT s.tx_hash
                 , s.block_timestamp
                 , s.buyer_address
                 , s.seller_address
                 , s.platform_name
                 , s.nft_address
                 , s.tokenid AS token_id
                 , s.price_usd
                 , s.creator_fee_usd
                 , s.platform_fee_usd
                 , (CASE WHEN s.currency_symbol IN ('ETH','WETH') THEN s.price
                         ELSE s.price_usd / p.price END) AS price_eth
                 , (CASE WHEN s.currency_symbol IN ('ETH','WETH') THEN s.creator_fee
                         ELSE s.creator_fee_usd / p.price END) AS creator_fee_eth
                 , (CASE WHEN s.currency_symbol IN ('ETH','WETH') THEN s.platform_fee
                         ELSE s.platform_fee_usd / p.price END) AS platform_fee_eth
              
            FROM ethereum.core.ez_nft_sales AS s
                LEFT JOIN ethereum.core.fact_hourly_token_prices AS p
                    ON p.hour = date_trunc('hour', s.block_timestamp)
                    AND p.symbol = 'WETH'
        
            WHERE True
          -- Exclude non-collectibles
              AND nft_address NOT IN ( '0xc36442b4a4522e871399cd717abdd847ab11fe88' -- Uniswap V3
                                     , '0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85' -- ENS
                                     , '0x58a3c68e2d3aaf316239c003779f71acb870ee47' -- Curve Finance
                                     )
           -- Filter out weird data points
              AND NOT (creator_fee < 0)
              AND s.price > 0
              AND NOT (currency_symbol = 'ETH' AND currency_address NOT IN ('ETH', '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'))  -- incorrect currency
          
          -- Dashboard filters
              AND block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
              AND (CASE WHEN lower('{Marketplace}') IN ('', '-', 'all', 'combined') THEN True
                        ELSE lower('{Marketplace}') = platform_name END)
              AND (CASE WHEN '{ContractAddress}' = '' THEN True
                        WHEN '{ContractAddress}' = '-' THEN True
                        WHEN '{ContractAddress}' = 'None' THEN True
                        WHEN '{ContractAddress}' = 'All' THEN True
                        WHEN '{ContractAddress}' IS NULL THEN True
                        ELSE lower('{ContractAddress}') = nft_address END)
        ),
        
        
        wash_trades AS (
         -- Query logic forked from:
         --   https://app.flipsidecrypto.com/velocity/queries/19bb605d-ef5b-4d76-b84e-386c93341f12
         --   by pinehearst
          
            WITH 
                
            eth_tx AS ( -- group day, A, B transfers 
                SELECT utc_date
                     , eth_from_address -- A 
                     , eth_to_address   -- B 
                     , sum(eth) as eth_transferred -- ETH/WETH transferred for the day 
                
                FROM (
                    SELECT tx_hash
                         , date_trunc('day', block_timestamp) AS utc_date
                         , eth_from_address -- A
                         , eth_to_address   -- B 
                         , amount AS eth 
                    FROM ethereum.core.ez_eth_transfers
                    WHERE block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
                      AND eth_from_address IN (SELECT DISTINCT buyer_address FROM ethereum.core.ez_nft_sales) -- filter out users that traded NFT 
                      AND tx_hash NOT IN (SELECT DISTINCT tx_hash FROM ethereum.core.ez_nft_sales) -- filter out ETH transfer from buying/selling 
                    
                    UNION 
                    
                    SELECT tx_hash
                         , date_trunc('day', block_timestamp) AS utc_date
                         , from_address AS eth_from_address -- A 
                         , to_address  AS eth_to_address    -- B 
                         , amount AS eth
                    FROM ethereum.core.ez_token_transfers
                    WHERE contract_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                      AND block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
                      AND from_address IN (SELECT DISTINCT buyer_address FROM ethereum.core.ez_nft_sales) -- filter out users that traded NFT 
                      AND tx_hash NOT IN (SELECT DISTINCT tx_hash FROM ethereum.core.ez_nft_sales) -- filter out WETH transfer from buying/selling i.e. back end WETH transfers 
                ) AS tx
                
                GROUP BY 1,2,3 
            ),
            
              
            pos_wash_tx AS (
                SELECT DISTINCT tx_hash
                FROM (
                    SELECT a.block_timestamp
                         , a.platform_name
                         , a.tx_hash
                         , datediff('minute', a.block_timestamp, b.block_timestamp) AS minutes_diff
                         , a.buyer_address AS buyerB -- B 
                         , b.seller_address AS sellerB -- B 
                         , a.seller_address AS sellerA -- A 
                         , b.buyer_address AS buyerA -- A 
                
                    FROM ethereum.core.ez_nft_sales AS a
                        LEFT JOIN ethereum.core.ez_nft_sales AS b 
                            ON a.nft_address = b.nft_address 
                            AND a.tokenid = b.tokenid 
                            AND a.buyer_address = b.seller_address -- buyer was a seller of the nft  	
                            AND a.seller_address = b.buyer_address -- seller was a buyer of the nft 
              
                    WHERE a.block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
                ) AS tx
                WHERE sellerB IS NOT NULL AND buyerA IS NOT NULL 
            ),
            
              
            labelled_trades AS (
                SELECT s.tx_hash
                     , s.block_timestamp
                     , s.platform_name
                     , s.buyer_address  -- B 
                     , s.seller_address -- A 
                     , etx.eth_transferred -- will be NULL if buyer and seller has no ETH/WETH transfer for the day 
                     , pw.tx_hash AS pos_wash_tx -- will be NULL if it wasn't a wash trading A->B->A 
                     , s.price
                     , s.price_usd
                     , s.nft_address
                     , s.project_name
                     , s.tokenid
              
                     , (CASE WHEN etx.eth_transferred IS NOT NULL AND pw.tx_hash IS NOT NULL THEN 'PoS & Self-Fund Wash' -- highly probably 
                             WHEN etx.eth_transferred IS NULL     AND pw.tx_hash IS NOT NULL THEN 'PoS Wash'             -- very likely wash  
                             WHEN etx.eth_transferred IS NOT NULL AND pw.tx_hash IS NULL     THEN 'Self-Fund Wash'       -- likely wash  
                             WHEN etx.eth_transferred IS NULL     AND pw.tx_hash IS NULL     THEN 'Organic'              -- organic   
                             END) AS sales_type
              
                FROM ethereum.core.ez_nft_sales AS s
                    LEFT JOIN eth_tx AS etx
                        ON s.buyer_address = etx.eth_to_address -- buyer (B) received ETH from...
                        AND s.seller_address = etx.eth_from_address -- seller (A)  
                        AND s.block_timestamp::date = etx.utc_date -- on the same day on the same day of sales 
                  
                    LEFT JOIN pos_wash_tx AS pw
                        ON pw.tx_hash = s.tx_hash
                
                WHERE s.block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
                    AND s.currency_symbol IN ('WETH', 'ETH') -- Filter sales in WETH/ETH
            )
            
            
            SELECT DISTINCT tx_hash, nft_address, tokenid
            FROM labelled_trades
            WHERE sales_type != 'Organic'
        ),
        
        
        nft_sales__wash_trade_filter_off AS (
            SELECT * 
            FROM nft_sales
        ),
        
        
        nft_sales__wash_trade_filter_on AS (
            SELECT * 
            FROM nft_sales
            WHERE tx_hash NOT IN (SELECT tx_hash FROM wash_trades)
        ),
        
        
        daily_trader_stats AS (
            SELECT utc_date
                 , trader
                 , sum(CASE WHEN tx_type = 'buy' THEN tx_count ELSE 0 END) AS buy_txs
                 , sum(CASE WHEN tx_type = 'buy' THEN volume_usd ELSE 0 END) AS buy_volume_usd
                 , sum(CASE WHEN tx_type = 'buy' THEN volume_eth ELSE 0 END) AS buy_volume_eth
                 , sum(CASE WHEN tx_type = 'sell' THEN tx_count ELSE 0 END) AS sell_txs
                 , sum(CASE WHEN tx_type = 'sell' THEN volume_usd ELSE 0 END) AS sell_volume_usd
                 , sum(CASE WHEN tx_type = 'sell' THEN volume_eth ELSE 0 END) AS sell_volume_eth
                 , sum(tx_count) AS total_txs
                 , sum(volume_usd) AS total_volume_usd
                 , sum(volume_eth) AS total_volume_eth
                 , sum(creator_fees_usd) AS total_creator_fees_usd
                 , sum(creator_fees_eth) AS total_creator_fees_eth
                 , sum(platform_fees_usd) AS total_platform_fees_usd
                 , sum(platform_fees_eth) AS total_platform_fees_eth
          
            FROM (
                SELECT block_timestamp::date AS utc_date
                     , buyer_address AS trader
                     , 'buy' AS tx_type
                     , count(*) AS tx_count
                     , sum(price_usd) AS volume_usd
                     , sum(price_eth) AS volume_eth
                     , sum(0) AS creator_fees_usd
                     , sum(0) AS creator_fees_eth
                     , sum(0) AS platform_fees_usd
                     , sum(0) AS platform_fees_eth
                FROM nft_sales__wash_trade_filter_{WashTradeFilter}
                GROUP BY 1,2,3
        
                UNION ALL
          
                SELECT block_timestamp::date AS utc_date
                     , seller_address AS trader
                     , 'sell' AS tx_type
                     , count(*) AS tx_count
                     , sum(price_usd) AS volume_usd
                     , sum(price_eth) AS volume_eth
                     , sum(creator_fee_usd) AS creator_fees_usd
                     , sum(creator_fee_eth) AS creator_fees_eth
                     , sum(platform_fee_usd) AS platform_fees_usd
                     , sum(platform_fee_eth) AS platform_fees_eth
                FROM nft_sales__wash_trade_filter_{WashTradeFilter}
                GROUP BY 1,2,3
            ) AS t
        
            GROUP BY 1,2
        ),
        
        
        daily_trading_wallets AS (
            SELECT utc_date
                 , sum(total_volume_usd) / 2 AS trading_volume__usd    -- divide by 2 to cancel double-counting due to buyer + seller
                 , sum(total_volume_eth) / 2 AS trading_volume__eth
                 , count(trader) AS total_trading_wallets
                 , sum(total_volume_usd) / count(trader) AS volume_per_wallet__usd
                 , sum(total_volume_eth) / count(trader) AS volume_per_wallet__eth
                 , sum(total_txs) / count(trader) AS transactions_per_wallet
                 , sum(total_volume_usd) / sum(total_txs) AS volume_per_tx__usd
                 , sum(total_volume_eth) / sum(total_txs) AS volume_per_tx__eth
                 , sum(total_creator_fees_usd) AS creator_fees__usd
                 , sum(total_creator_fees_eth) AS creator_fees__eth
                 , sum(total_platform_fees_usd) AS platform_fees__usd
                 , sum(total_platform_fees_eth) AS platform_fees__eth
        
                 , trading_volume__{Currency} AS trading_volume
                 , volume_per_wallet__{Currency} AS volume_per_wallet
                 , volume_per_tx__{Currency} AS volume_per_tx
                 , creator_fees__{Currency} AS creator_fees
                 , platform_fees__{Currency} AS platform_fees
          
            FROM daily_trader_stats
            WHERE total_txs > 0
            GROUP BY 1
        ),
        
        
        daily_trading_wallets__annotated AS (
            SELECT *
        
                 , (CASE WHEN utc_date BETWEEN CURRENT_DATE - interval '{TimePeriodDays} days'
                                           AND CURRENT_DATE - interval '1 day' 
                                          THEN 'Current Period'
          
                         WHEN utc_date BETWEEN CURRENT_DATE - interval '{TimePeriodDays} days' - interval '{TimePeriodDays} days' 
                                           AND CURRENT_DATE - interval '{TimePeriodDays} days' - interval '1 day'
                                          THEN 'Prior Period'
          
                         WHEN utc_date BETWEEN CURRENT_DATE - interval '1 year' - interval '{TimePeriodDays} days'
                                           AND CURRENT_DATE - interval '1 year' - interval '1 day'
                                          THEN 'Prior Year'
          
                         ELSE '-' END) AS period
          
            FROM daily_trading_wallets
        )
            
        
        SELECT *
        FROM daily_trading_wallets__annotated
        ORDER BY utc_date DESC
        LIMIT 10000
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


def get_nft_trading_volume(
        TimePeriodDays: int = 365,
        Marketplace: str = "All",
        ContractAddress: str = "",
        WashTradeFilter: str = "On",
        Currency: str = "USD") -> pd.DataFrame:
    """

    :return:
    """
    query = f"""
    WITH
        nft_sales AS (
            SELECT s.tx_hash
                 , s.block_timestamp
                 , s.buyer_address
                 , s.seller_address
                 , s.platform_name
                 , s.nft_address
                 , s.tokenid AS token_id
                 , s.price_usd
                 , s.creator_fee_usd
                 , s.platform_fee_usd
                 , (CASE WHEN s.currency_symbol IN ('ETH','WETH') THEN s.price
                         ELSE s.price_usd / p.price END) AS price_eth
                 , (CASE WHEN s.currency_symbol IN ('ETH','WETH') THEN s.creator_fee
                         ELSE s.creator_fee_usd / p.price END) AS creator_fee_eth
                 , (CASE WHEN s.currency_symbol IN ('ETH','WETH') THEN s.platform_fee
                         ELSE s.platform_fee_usd / p.price END) AS platform_fee_eth

            FROM ethereum.core.ez_nft_sales AS s
                LEFT JOIN ethereum.core.fact_hourly_token_prices AS p
                    ON p.hour = date_trunc('hour', s.block_timestamp)
                    AND p.symbol = 'WETH'

            WHERE True
          -- Exclude non-collectibles
              AND nft_address NOT IN ( '0xc36442b4a4522e871399cd717abdd847ab11fe88' -- Uniswap V3
                                     , '0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85' -- ENS
                                     , '0x58a3c68e2d3aaf316239c003779f71acb870ee47' -- Curve Finance
                                     )
           -- Filter out weird data points
              AND NOT (creator_fee < 0)
              AND s.price > 0
              AND NOT (currency_symbol = 'ETH' AND currency_address NOT IN ('ETH', '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'))  -- incorrect currency

          -- Dashboard filters
              AND block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
              AND (CASE WHEN lower('{Marketplace}') IN ('', '-', 'all', 'combined') THEN True
                        ELSE lower('{Marketplace}') = platform_name END)
              AND (CASE WHEN '{ContractAddress}' = '' THEN True
                        WHEN '{ContractAddress}' = '-' THEN True
                        WHEN '{ContractAddress}' = 'None' THEN True
                        WHEN '{ContractAddress}' = 'All' THEN True
                        WHEN '{ContractAddress}' IS NULL THEN True
                        ELSE lower('{ContractAddress}') = nft_address END)
        ),


        wash_trades AS (
         -- Query logic forked from:
         --   https://app.flipsidecrypto.com/velocity/queries/19bb605d-ef5b-4d76-b84e-386c93341f12
         --   by pinehearst

            WITH 

            eth_tx AS ( -- group day, A, B transfers 
                SELECT utc_date
                     , eth_from_address -- A 
                     , eth_to_address   -- B 
                     , sum(eth) as eth_transferred -- ETH/WETH transferred for the day 

                FROM (
                    SELECT tx_hash
                         , date_trunc('day', block_timestamp) AS utc_date
                         , eth_from_address -- A
                         , eth_to_address   -- B 
                         , amount AS eth 
                    FROM ethereum.core.ez_eth_transfers
                    WHERE block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
                      AND eth_from_address IN (SELECT DISTINCT buyer_address FROM ethereum.core.ez_nft_sales) -- filter out users that traded NFT 
                      AND tx_hash NOT IN (SELECT DISTINCT tx_hash FROM ethereum.core.ez_nft_sales) -- filter out ETH transfer from buying/selling 

                    UNION 

                    SELECT tx_hash
                         , date_trunc('day', block_timestamp) AS utc_date
                         , from_address AS eth_from_address -- A 
                         , to_address  AS eth_to_address    -- B 
                         , amount AS eth
                    FROM ethereum.core.ez_token_transfers
                    WHERE contract_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                      AND block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
                      AND from_address IN (SELECT DISTINCT buyer_address FROM ethereum.core.ez_nft_sales) -- filter out users that traded NFT 
                      AND tx_hash NOT IN (SELECT DISTINCT tx_hash FROM ethereum.core.ez_nft_sales) -- filter out WETH transfer from buying/selling i.e. back end WETH transfers 
                ) AS tx

                GROUP BY 1,2,3 
            ),


            pos_wash_tx AS (
                SELECT DISTINCT tx_hash
                FROM (
                    SELECT a.block_timestamp
                         , a.platform_name
                         , a.tx_hash
                         , datediff('minute', a.block_timestamp, b.block_timestamp) AS minutes_diff
                         , a.buyer_address AS buyerB -- B 
                         , b.seller_address AS sellerB -- B 
                         , a.seller_address AS sellerA -- A 
                         , b.buyer_address AS buyerA -- A 

                    FROM ethereum.core.ez_nft_sales AS a
                        LEFT JOIN ethereum.core.ez_nft_sales AS b 
                            ON a.nft_address = b.nft_address 
                            AND a.tokenid = b.tokenid 
                            AND a.buyer_address = b.seller_address -- buyer was a seller of the nft  	
                            AND a.seller_address = b.buyer_address -- seller was a buyer of the nft 

                    WHERE a.block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
                ) AS tx
                WHERE sellerB IS NOT NULL AND buyerA IS NOT NULL 
            ),


            labelled_trades AS (
                SELECT s.tx_hash
                     , s.block_timestamp
                     , s.platform_name
                     , s.buyer_address  -- B 
                     , s.seller_address -- A 
                     , etx.eth_transferred -- will be NULL if buyer and seller has no ETH/WETH transfer for the day 
                     , pw.tx_hash AS pos_wash_tx -- will be NULL if it wasn't a wash trading A->B->A 
                     , s.price
                     , s.price_usd
                     , s.nft_address
                     , s.project_name
                     , s.tokenid

                     , (CASE WHEN etx.eth_transferred IS NOT NULL AND pw.tx_hash IS NOT NULL THEN 'PoS & Self-Fund Wash' -- highly probably 
                             WHEN etx.eth_transferred IS NULL     AND pw.tx_hash IS NOT NULL THEN 'PoS Wash'             -- very likely wash  
                             WHEN etx.eth_transferred IS NOT NULL AND pw.tx_hash IS NULL     THEN 'Self-Fund Wash'       -- likely wash  
                             WHEN etx.eth_transferred IS NULL     AND pw.tx_hash IS NULL     THEN 'Organic'              -- organic   
                             END) AS sales_type

                FROM ethereum.core.ez_nft_sales AS s
                    LEFT JOIN eth_tx AS etx
                        ON s.buyer_address = etx.eth_to_address -- buyer (B) received ETH from...
                        AND s.seller_address = etx.eth_from_address -- seller (A)  
                        AND s.block_timestamp::date = etx.utc_date -- on the same day on the same day of sales 

                    LEFT JOIN pos_wash_tx AS pw
                        ON pw.tx_hash = s.tx_hash

                WHERE s.block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
                    AND s.currency_symbol IN ('WETH', 'ETH') -- Filter sales in WETH/ETH
            )


            SELECT DISTINCT tx_hash, nft_address, tokenid
            FROM labelled_trades
            WHERE sales_type != 'Organic'
        ),


        nft_sales__wash_trade_filter_off AS (
            SELECT * 
            FROM nft_sales
        ),


        nft_sales__wash_trade_filter_on AS (
            SELECT * 
            FROM nft_sales
            WHERE tx_hash NOT IN (SELECT tx_hash FROM wash_trades)
        ),


        daily_trader_stats AS (
            SELECT utc_date
                 , trader

                 , (CASE WHEN utc_date BETWEEN CURRENT_DATE - interval '{TimePeriodDays} days'
                                           AND CURRENT_DATE - interval '1 day' 
                                          THEN 'Current Period'

                         WHEN utc_date BETWEEN CURRENT_DATE - interval '{TimePeriodDays} days' - interval '{TimePeriodDays} days' 
                                           AND CURRENT_DATE - interval '{TimePeriodDays} days' - interval '1 day'
                                          THEN 'Prior Period'

                         WHEN utc_date BETWEEN CURRENT_DATE - interval '1 year' - interval '{TimePeriodDays} days'
                                           AND CURRENT_DATE - interval '1 year' - interval '1 day'
                                          THEN 'Prior Year'

                         ELSE '-' END) AS period

                 , sum(CASE WHEN tx_type = 'buy' THEN tx_count ELSE 0 END) AS buy_txs
                 , sum(CASE WHEN tx_type = 'buy' THEN volume_usd ELSE 0 END) AS buy_volume_usd
                 , sum(CASE WHEN tx_type = 'buy' THEN volume_eth ELSE 0 END) AS buy_volume_eth
                 , sum(CASE WHEN tx_type = 'sell' THEN tx_count ELSE 0 END) AS sell_txs
                 , sum(CASE WHEN tx_type = 'sell' THEN volume_usd ELSE 0 END) AS sell_volume_usd
                 , sum(CASE WHEN tx_type = 'sell' THEN volume_eth ELSE 0 END) AS sell_volume_eth
                 , sum(tx_count) AS total_txs
                 , sum(volume_usd) AS total_volume_usd
                 , sum(volume_eth) AS total_volume_eth

            FROM (
                SELECT block_timestamp::date AS utc_date
                     , buyer_address AS trader
                     , 'buy' AS tx_type
                     , count(*) AS tx_count
                     , sum(price_usd) AS volume_usd
                     , sum(price_eth) AS volume_eth
                FROM nft_sales__wash_trade_filter_{WashTradeFilter}
                GROUP BY 1,2,3

                UNION ALL

                SELECT block_timestamp::date AS utc_date
                     , seller_address AS trader
                     , 'sell' AS tx_type
                     , count(*) AS tx_count
                     , sum(price_usd) AS volume_usd
                     , sum(price_eth) AS volume_eth
                FROM nft_sales__wash_trade_filter_{WashTradeFilter}
                GROUP BY 1,2,3
            ) AS t

            GROUP BY 1,2,3
        ),


        trader_summary_stats AS (
            SELECT trader
                 , period
                 , sum(total_txs) AS count_trades
                 , sum(total_volume_usd) AS volume_usd
                 , sum(total_volume_eth) AS volume_eth
            FROM daily_trader_stats
            WHERE period IN ('Current Period', 'Prior Period', 'Prior Year')
            GROUP BY 1,2
        ),


        period_summary_stats AS (
            SELECT period
                 , count(trader) AS wallets
                 , count(CASE WHEN count_trades = 1 THEN trader ELSE NULL END) AS wallets__single_tx
                 , count(CASE WHEN count_trades > 1 THEN trader ELSE NULL END) AS wallets__multiple_tx

                 , sum(volume_usd) / 2 AS trading_volume__usd
                 , sum(volume_eth) / 2 AS trading_volume__eth

                 , sum(volume_usd) / count(trader) AS volume_per_wallet__usd
                 , sum(volume_eth) / count(trader) AS volume_per_wallet__eth
                 , sum(count_trades) / count(trader) AS transactions_per_wallet
                 , sum(volume_usd) / sum(count_trades) AS volume_per_transaction__usd
                 , sum(volume_eth) / sum(count_trades) AS volume_per_transaction__eth

                 , trading_volume__{Currency} AS trading_volume
                 , volume_per_wallet__{Currency} AS volume_per_wallet
                 , volume_per_transaction__{Currency} AS volume_per_transaction

            FROM trader_summary_stats
            WHERE count_trades >= 1 
            GROUP BY 1
        ),


        metric_pivot AS (
            SELECT 'Volume per Wallet' AS metric
                 , (CASE WHEN '{Currency}' = 'ETH' THEN 'Ξ'
                         WHEN '{Currency}' = 'USD' THEN '$' END) AS currency_symbol
                 , avg(CASE WHEN period = 'Current Period' THEN trading_volume ELSE NULL END) AS value__current_period
                 , avg(CASE WHEN period = 'Prior Period' THEN trading_volume ELSE NULL END) AS value__prior_period
                 , avg(CASE WHEN period = 'Prior Year' THEN trading_volume ELSE NULL END) AS value__prior_year
                 , 100.00 * (value__current_period / nullif(value__prior_period, 0) - 1) AS perc_change_vs_prior_period
                 , 100.00 * (value__current_period / nullif(value__prior_year, 0) - 1) AS perc_change_vs_prior_year
            FROM period_summary_stats
            GROUP BY 1,2
        ),


        unpivot AS (
            SELECT 1 AS _order_
                 , 'Current Period' AS metric
                 , concat(currency_symbol, ' ', to_varchar(value__current_period, '999,999,999,999.0000')) AS value_
            FROM metric_pivot

            UNION ALL

            SELECT 2 AS _order_
                 , 'Prior Period' AS metric
                 , concat(currency_symbol, ' ', to_varchar(value__prior_period, '999,999,999,999.0000')) AS value_
            FROM metric_pivot

            UNION ALL

            SELECT 3 AS _order_
                 , 'Prior Year' AS metric
                 , concat(currency_symbol, ' ', to_varchar(value__prior_year, '999,999,999,999.0000')) AS value_
            FROM metric_pivot

            UNION ALL

            SELECT 4 AS _order_
                 , '% Change vs Prior Period' AS metric
                 , concat(to_varchar(perc_change_vs_prior_period, 'S999,999,999,999.00'), '%') AS value_
            FROM metric_pivot

            UNION ALL

            SELECT 5 AS _order_
                 , '% Change vs Prior Year' AS metric
                 , concat(to_varchar(perc_change_vs_prior_year, 'S999,999,999,999.00'), '%') AS value_
            FROM metric_pivot
        )


        SELECT metric AS "Metric"
             , value_ AS "Value"
        FROM unpivot
        ORDER BY _order_
        LIMIT 10000
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


def get_uniq_trdng_wlt_stats(
        TimePeriodDays: int = 365,
        Marketplace: str = "All",
        ContractAddress: str = "",
        WashTradeFilter: str = "On",
        Currency: str = "USD") -> pd.DataFrame:
    """

    :return:
    """
    query = f"""
    WITH
        nft_sales AS (
            SELECT s.tx_hash
                 , s.block_timestamp
                 , s.buyer_address
                 , s.seller_address
                 , s.platform_name
                 , s.nft_address
                 , s.tokenid AS token_id
                 , s.price_usd
                 , s.creator_fee_usd
                 , s.platform_fee_usd
                 , (CASE WHEN s.currency_symbol IN ('ETH','WETH') THEN s.price
                         ELSE s.price_usd / p.price END) AS price_eth
                 , (CASE WHEN s.currency_symbol IN ('ETH','WETH') THEN s.creator_fee
                         ELSE s.creator_fee_usd / p.price END) AS creator_fee_eth
                 , (CASE WHEN s.currency_symbol IN ('ETH','WETH') THEN s.platform_fee
                         ELSE s.platform_fee_usd / p.price END) AS platform_fee_eth
              
            FROM ethereum.core.ez_nft_sales AS s
                LEFT JOIN ethereum.core.fact_hourly_token_prices AS p
                    ON p.hour = date_trunc('hour', s.block_timestamp)
                    AND p.symbol = 'WETH'
        
            WHERE True
          -- Exclude non-collectibles
              AND nft_address NOT IN ( '0xc36442b4a4522e871399cd717abdd847ab11fe88' -- Uniswap V3
                                     , '0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85' -- ENS
                                     , '0x58a3c68e2d3aaf316239c003779f71acb870ee47' -- Curve Finance
                                     )
           -- Filter out weird data points
              AND NOT (creator_fee < 0)
              AND s.price > 0
              AND NOT (currency_symbol = 'ETH' AND currency_address NOT IN ('ETH', '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'))  -- incorrect currency
          
          -- Dashboard filters
              AND block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
              AND (CASE WHEN lower('{Marketplace}') IN ('', '-', 'all', 'combined') THEN True
                        ELSE lower('{Marketplace}') = platform_name END)
              AND (CASE WHEN '{ContractAddress}' = '' THEN True
                        WHEN '{ContractAddress}' = '-' THEN True
                        WHEN '{ContractAddress}' = 'None' THEN True
                        WHEN '{ContractAddress}' = 'All' THEN True
                        WHEN '{ContractAddress}' IS NULL THEN True
                        ELSE lower('{ContractAddress}') = nft_address END)
        ),
        
        
        wash_trades AS (
         -- Query logic forked from:
         --   https://app.flipsidecrypto.com/velocity/queries/19bb605d-ef5b-4d76-b84e-386c93341f12
         --   by pinehearst
          
            WITH 
                
            eth_tx AS ( -- group day, A, B transfers 
                SELECT utc_date
                     , eth_from_address -- A 
                     , eth_to_address   -- B 
                     , sum(eth) as eth_transferred -- ETH/WETH transferred for the day 
                
                FROM (
                    SELECT tx_hash
                         , date_trunc('day', block_timestamp) AS utc_date
                         , eth_from_address -- A
                         , eth_to_address   -- B 
                         , amount AS eth 
                    FROM ethereum.core.ez_eth_transfers
                    WHERE block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
                      AND eth_from_address IN (SELECT DISTINCT buyer_address FROM ethereum.core.ez_nft_sales) -- filter out users that traded NFT 
                      AND tx_hash NOT IN (SELECT DISTINCT tx_hash FROM ethereum.core.ez_nft_sales) -- filter out ETH transfer from buying/selling 
                    
                    UNION 
                    
                    SELECT tx_hash
                         , date_trunc('day', block_timestamp) AS utc_date
                         , from_address AS eth_from_address -- A 
                         , to_address  AS eth_to_address    -- B 
                         , amount AS eth
                    FROM ethereum.core.ez_token_transfers
                    WHERE contract_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                      AND block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
                      AND from_address IN (SELECT DISTINCT buyer_address FROM ethereum.core.ez_nft_sales) -- filter out users that traded NFT 
                      AND tx_hash NOT IN (SELECT DISTINCT tx_hash FROM ethereum.core.ez_nft_sales) -- filter out WETH transfer from buying/selling i.e. back end WETH transfers 
                ) AS tx
                
                GROUP BY 1,2,3 
            ),
            
              
            pos_wash_tx AS (
                SELECT DISTINCT tx_hash
                FROM (
                    SELECT a.block_timestamp
                         , a.platform_name
                         , a.tx_hash
                         , datediff('minute', a.block_timestamp, b.block_timestamp) AS minutes_diff
                         , a.buyer_address AS buyerB -- B 
                         , b.seller_address AS sellerB -- B 
                         , a.seller_address AS sellerA -- A 
                         , b.buyer_address AS buyerA -- A 
                
                    FROM ethereum.core.ez_nft_sales AS a
                        LEFT JOIN ethereum.core.ez_nft_sales AS b 
                            ON a.nft_address = b.nft_address 
                            AND a.tokenid = b.tokenid 
                            AND a.buyer_address = b.seller_address -- buyer was a seller of the nft  	
                            AND a.seller_address = b.buyer_address -- seller was a buyer of the nft 
              
                    WHERE a.block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
                ) AS tx
                WHERE sellerB IS NOT NULL AND buyerA IS NOT NULL 
            ),
            
              
            labelled_trades AS (
                SELECT s.tx_hash
                     , s.block_timestamp
                     , s.platform_name
                     , s.buyer_address  -- B 
                     , s.seller_address -- A 
                     , etx.eth_transferred -- will be NULL if buyer and seller has no ETH/WETH transfer for the day 
                     , pw.tx_hash AS pos_wash_tx -- will be NULL if it wasn't a wash trading A->B->A 
                     , s.price
                     , s.price_usd
                     , s.nft_address
                     , s.project_name
                     , s.tokenid
              
                     , (CASE WHEN etx.eth_transferred IS NOT NULL AND pw.tx_hash IS NOT NULL THEN 'PoS & Self-Fund Wash' -- highly probably 
                             WHEN etx.eth_transferred IS NULL     AND pw.tx_hash IS NOT NULL THEN 'PoS Wash'             -- very likely wash  
                             WHEN etx.eth_transferred IS NOT NULL AND pw.tx_hash IS NULL     THEN 'Self-Fund Wash'       -- likely wash  
                             WHEN etx.eth_transferred IS NULL     AND pw.tx_hash IS NULL     THEN 'Organic'              -- organic   
                             END) AS sales_type
              
                FROM ethereum.core.ez_nft_sales AS s
                    LEFT JOIN eth_tx AS etx
                        ON s.buyer_address = etx.eth_to_address -- buyer (B) received ETH from...
                        AND s.seller_address = etx.eth_from_address -- seller (A)  
                        AND s.block_timestamp::date = etx.utc_date -- on the same day on the same day of sales 
                  
                    LEFT JOIN pos_wash_tx AS pw
                        ON pw.tx_hash = s.tx_hash
                
                WHERE s.block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
                    AND s.currency_symbol IN ('WETH', 'ETH') -- Filter sales in WETH/ETH
            )
            
            
            SELECT DISTINCT tx_hash, nft_address, tokenid
            FROM labelled_trades
            WHERE sales_type != 'Organic'
        ),
        
        
        nft_sales__wash_trade_filter_off AS (
            SELECT * 
            FROM nft_sales
        ),
        
        
        nft_sales__wash_trade_filter_on AS (
            SELECT * 
            FROM nft_sales
            WHERE tx_hash NOT IN (SELECT tx_hash FROM wash_trades)
        ),
        
        
        daily_trader_stats AS (
            SELECT utc_date
                 , trader
        
                 , (CASE WHEN utc_date BETWEEN CURRENT_DATE - interval '{TimePeriodDays} days'
                                           AND CURRENT_DATE - interval '1 day' 
                                          THEN 'Current Period'
          
                         WHEN utc_date BETWEEN CURRENT_DATE - interval '{TimePeriodDays} days' - interval '{TimePeriodDays} days' 
                                           AND CURRENT_DATE - interval '{TimePeriodDays} days' - interval '1 day'
                                          THEN 'Prior Period'
          
                         WHEN utc_date BETWEEN CURRENT_DATE - interval '1 year' - interval '{TimePeriodDays} days'
                                           AND CURRENT_DATE - interval '1 year' - interval '1 day'
                                          THEN 'Prior Year'
          
                         ELSE '-' END) AS period
          
                 , sum(CASE WHEN tx_type = 'buy' THEN tx_count ELSE 0 END) AS buy_txs
                 , sum(CASE WHEN tx_type = 'buy' THEN volume_usd ELSE 0 END) AS buy_volume_usd
                 , sum(CASE WHEN tx_type = 'buy' THEN volume_eth ELSE 0 END) AS buy_volume_eth
                 , sum(CASE WHEN tx_type = 'sell' THEN tx_count ELSE 0 END) AS sell_txs
                 , sum(CASE WHEN tx_type = 'sell' THEN volume_usd ELSE 0 END) AS sell_volume_usd
                 , sum(CASE WHEN tx_type = 'sell' THEN volume_eth ELSE 0 END) AS sell_volume_eth
                 , sum(tx_count) AS total_txs
                 , sum(volume_usd) AS total_volume_usd
                 , sum(volume_eth) AS total_volume_eth
          
            FROM (
                SELECT block_timestamp::date AS utc_date
                     , buyer_address AS trader
                     , 'buy' AS tx_type
                     , count(*) AS tx_count
                     , sum(price_usd) AS volume_usd
                     , sum(price_eth) AS volume_eth
                FROM nft_sales__wash_trade_filter_{WashTradeFilter}
                GROUP BY 1,2,3
        
                UNION ALL
          
                SELECT block_timestamp::date AS utc_date
                     , seller_address AS trader
                     , 'sell' AS tx_type
                     , count(*) AS tx_count
                     , sum(price_usd) AS volume_usd
                     , sum(price_eth) AS volume_eth
                FROM nft_sales__wash_trade_filter_{WashTradeFilter}
                GROUP BY 1,2,3
            ) AS t
        
            GROUP BY 1,2,3
        ),
        
        
        trader_summary_stats AS (
            SELECT trader
                 , period
                 , sum(total_txs) AS count_trades
                 , sum(total_volume_usd) AS volume_usd
                 , sum(total_volume_eth) AS volume_eth
            FROM daily_trader_stats
            WHERE period IN ('Current Period', 'Prior Period', 'Prior Year')
            GROUP BY 1,2
        ),
        
        
        period_summary_stats AS (
            SELECT period
                 , count(trader) AS wallets
                 , count(CASE WHEN count_trades = 1 THEN trader ELSE NULL END) AS wallets__single_tx
                 , count(CASE WHEN count_trades > 1 THEN trader ELSE NULL END) AS wallets__multiple_tx
          
                 , sum(volume_usd) / 2 AS trading_volume__usd
                 , sum(volume_eth) / 2 AS trading_volume__eth
          
                 , sum(volume_usd) / count(trader) AS volume_per_wallet__usd
                 , sum(volume_eth) / count(trader) AS volume_per_wallet__eth
                 , sum(count_trades) / count(trader) AS transactions_per_wallet
                 , sum(volume_usd) / sum(count_trades) AS volume_per_transaction__usd
                 , sum(volume_eth) / sum(count_trades) AS volume_per_transaction__eth
        
                 , trading_volume__{Currency} AS trading_volume
                 , volume_per_wallet__{Currency} AS volume_per_wallet
                 , volume_per_transaction__{Currency} AS volume_per_transaction
          
            FROM trader_summary_stats
            WHERE count_trades >= 1 
            GROUP BY 1
        ),
        
        
        metric_pivot AS (
            SELECT 'Trading Wallets' AS metric
                 , avg(CASE WHEN period = 'Current Period' THEN wallets ELSE NULL END) AS value__current_period
                 , avg(CASE WHEN period = 'Prior Period'   THEN wallets ELSE NULL END) AS value__prior_period
                 , avg(CASE WHEN period = 'Prior Year'     THEN wallets ELSE NULL END) AS value__prior_year
                 , 100.00 * (value__current_period / value__prior_period - 1) AS perc_change_vs_prior_period
                 , 100.00 * (value__current_period / value__prior_year - 1)   AS perc_change_vs_prior_year
          
                 , avg(CASE WHEN period = 'Current Period' THEN wallets__single_tx ELSE NULL END) AS value__current_period__single_tx
                 , avg(CASE WHEN period = 'Prior Period'   THEN wallets__single_tx ELSE NULL END) AS value__prior_period__single_tx
                 , avg(CASE WHEN period = 'Prior Year'     THEN wallets__single_tx ELSE NULL END) AS value__prior_year__single_tx
                 , 100.00 * (value__current_period__single_tx / value__prior_period__single_tx - 1) AS perc_change_vs_prior_period__single_tx
                 , 100.00 * (value__current_period__single_tx / value__prior_year__single_tx - 1)   AS perc_change_vs_prior_year__single_tx
          
                 , avg(CASE WHEN period = 'Current Period' THEN wallets__multiple_tx ELSE NULL END) AS value__current_period__multiple_tx
                 , avg(CASE WHEN period = 'Prior Period'   THEN wallets__multiple_tx ELSE NULL END) AS value__prior_period__multiple_tx
                 , avg(CASE WHEN period = 'Prior Year'     THEN wallets__multiple_tx ELSE NULL END) AS value__prior_year__multiple_tx
                 , 100.00 * (value__current_period__multiple_tx / nullif(value__prior_period__multiple_tx, 0) - 1) AS perc_change_vs_prior_period__multiple_tx
                 , 100.00 * (value__current_period__multiple_tx / nullif(value__prior_year__multiple_tx, 0) - 1)   AS perc_change_vs_prior_year__multiple_tx
            
            FROM period_summary_stats
            GROUP BY 1
        ),
        
        
        unpivot AS (
            SELECT 1 AS _order_
                 , 'Current Period' AS metric
                 , to_varchar(value__current_period, '999,999,999,999') AS value_
                 , to_varchar(value__current_period__single_tx, '999,999,999,999') AS value__single_tx
                 , to_varchar(value__current_period__multiple_tx, '999,999,999,999') AS value__multiple_tx
            FROM metric_pivot
        
            UNION ALL
          
            SELECT 2 AS _order_
                 , 'Prior Period' AS metric
                 , to_varchar(value__prior_period, '999,999,999,999') AS value_
                 , to_varchar(value__prior_period__single_tx, '999,999,999,999') AS value__single_tx
                 , to_varchar(value__prior_period__multiple_tx, '999,999,999,999') AS value__multiple_tx
            FROM metric_pivot
        
            UNION ALL
          
            SELECT 3 AS _order_
                 , 'Prior Year' AS metric
                 , to_varchar(value__prior_year, '999,999,999,999') AS value_
                 , to_varchar(value__prior_year__single_tx, '999,999,999,999') AS value__single_tx
                 , to_varchar(value__prior_year__multiple_tx, '999,999,999,999') AS value__multiple_tx
            FROM metric_pivot
        
            UNION ALL
          
            SELECT 4 AS _order_
                 , '% Change vs Prior Period' AS metric
                 , concat(to_varchar(perc_change_vs_prior_period, 'S999,999,999,999.00'), '%') AS value_
                 , concat(to_varchar(perc_change_vs_prior_period__single_tx, 'S999,999,999,999.00'), '%') AS value__single_tx
                 , concat(to_varchar(perc_change_vs_prior_period__multiple_tx, 'S999,999,999,999.00'), '%') AS value__multiple_tx
            FROM metric_pivot
        
            UNION ALL
          
            SELECT 5 AS _order_
                 , '% Change vs Prior Year' AS metric
                 , concat(to_varchar(perc_change_vs_prior_year, 'S999,999,999,999.00'), '%') AS value_
                 , concat(to_varchar(perc_change_vs_prior_year__single_tx, 'S999,999,999,999.00'), '%') AS value__single_tx
                 , concat(to_varchar(perc_change_vs_prior_year__multiple_tx, 'S999,999,999,999.00'), '%') AS value__multiple_tx
            FROM metric_pivot
        )
        
        
        SELECT metric AS "Metric"
             , value_ AS "All Wallets"
             , value__single_tx AS "Single TX"
             , value__multiple_tx AS "Multiple TX"
        FROM unpivot
        ORDER BY _order_
        LIMIT 10000
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


def get_vol_per_wlt_stats(
        TimePeriodDays: int = 365,
        Marketplace: str = "All",
        ContractAddress: str = "",
        WashTradeFilter: str = "On",
        Currency: str = "USD") -> pd.DataFrame:
    """

    :return:
    """
    query = f"""
    WITH
        nft_sales AS (
            SELECT s.tx_hash
                 , s.block_timestamp
                 , s.buyer_address
                 , s.seller_address
                 , s.platform_name
                 , s.nft_address
                 , s.tokenid AS token_id
                 , s.price_usd
                 , s.creator_fee_usd
                 , s.platform_fee_usd
                 , (CASE WHEN s.currency_symbol IN ('ETH','WETH') THEN s.price
                         ELSE s.price_usd / p.price END) AS price_eth
                 , (CASE WHEN s.currency_symbol IN ('ETH','WETH') THEN s.creator_fee
                         ELSE s.creator_fee_usd / p.price END) AS creator_fee_eth
                 , (CASE WHEN s.currency_symbol IN ('ETH','WETH') THEN s.platform_fee
                         ELSE s.platform_fee_usd / p.price END) AS platform_fee_eth
              
            FROM ethereum.core.ez_nft_sales AS s
                LEFT JOIN ethereum.core.fact_hourly_token_prices AS p
                    ON p.hour = date_trunc('hour', s.block_timestamp)
                    AND p.symbol = 'WETH'
        
            WHERE True
          -- Exclude non-collectibles
              AND nft_address NOT IN ( '0xc36442b4a4522e871399cd717abdd847ab11fe88' -- Uniswap V3
                                     , '0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85' -- ENS
                                     , '0x58a3c68e2d3aaf316239c003779f71acb870ee47' -- Curve Finance
                                     )
           -- Filter out weird data points
              AND NOT (creator_fee < 0)
              AND s.price > 0
              AND NOT (currency_symbol = 'ETH' AND currency_address NOT IN ('ETH', '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'))  -- incorrect currency
          
          -- Dashboard filters
              AND block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
              AND (CASE WHEN lower('{Marketplace}') IN ('', '-', 'all', 'combined') THEN True
                        ELSE lower('{Marketplace}') = platform_name END)
              AND (CASE WHEN '{ContractAddress}' = '' THEN True
                        WHEN '{ContractAddress}' = '-' THEN True
                        WHEN '{ContractAddress}' = 'None' THEN True
                        WHEN '{ContractAddress}' = 'All' THEN True
                        WHEN '{ContractAddress}' IS NULL THEN True
                        ELSE lower('{ContractAddress}') = nft_address END)
        ),
        
        
        wash_trades AS (
         -- Query logic forked from:
         --   https://app.flipsidecrypto.com/velocity/queries/19bb605d-ef5b-4d76-b84e-386c93341f12
         --   by pinehearst
          
            WITH 
                
            eth_tx AS ( -- group day, A, B transfers 
                SELECT utc_date
                     , eth_from_address -- A 
                     , eth_to_address   -- B 
                     , sum(eth) as eth_transferred -- ETH/WETH transferred for the day 
                
                FROM (
                    SELECT tx_hash
                         , date_trunc('day', block_timestamp) AS utc_date
                         , eth_from_address -- A
                         , eth_to_address   -- B 
                         , amount AS eth 
                    FROM ethereum.core.ez_eth_transfers
                    WHERE block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
                      AND eth_from_address IN (SELECT DISTINCT buyer_address FROM ethereum.core.ez_nft_sales) -- filter out users that traded NFT 
                      AND tx_hash NOT IN (SELECT DISTINCT tx_hash FROM ethereum.core.ez_nft_sales) -- filter out ETH transfer from buying/selling 
                    
                    UNION 
                    
                    SELECT tx_hash
                         , date_trunc('day', block_timestamp) AS utc_date
                         , from_address AS eth_from_address -- A 
                         , to_address  AS eth_to_address    -- B 
                         , amount AS eth
                    FROM ethereum.core.ez_token_transfers
                    WHERE contract_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                      AND block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
                      AND from_address IN (SELECT DISTINCT buyer_address FROM ethereum.core.ez_nft_sales) -- filter out users that traded NFT 
                      AND tx_hash NOT IN (SELECT DISTINCT tx_hash FROM ethereum.core.ez_nft_sales) -- filter out WETH transfer from buying/selling i.e. back end WETH transfers 
                ) AS tx
                
                GROUP BY 1,2,3 
            ),
            
              
            pos_wash_tx AS (
                SELECT DISTINCT tx_hash
                FROM (
                    SELECT a.block_timestamp
                         , a.platform_name
                         , a.tx_hash
                         , datediff('minute', a.block_timestamp, b.block_timestamp) AS minutes_diff
                         , a.buyer_address AS buyerB -- B 
                         , b.seller_address AS sellerB -- B 
                         , a.seller_address AS sellerA -- A 
                         , b.buyer_address AS buyerA -- A 
                
                    FROM ethereum.core.ez_nft_sales AS a
                        LEFT JOIN ethereum.core.ez_nft_sales AS b 
                            ON a.nft_address = b.nft_address 
                            AND a.tokenid = b.tokenid 
                            AND a.buyer_address = b.seller_address -- buyer was a seller of the nft  	
                            AND a.seller_address = b.buyer_address -- seller was a buyer of the nft 
              
                    WHERE a.block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
                ) AS tx
                WHERE sellerB IS NOT NULL AND buyerA IS NOT NULL 
            ),
            
              
            labelled_trades AS (
                SELECT s.tx_hash
                     , s.block_timestamp
                     , s.platform_name
                     , s.buyer_address  -- B 
                     , s.seller_address -- A 
                     , etx.eth_transferred -- will be NULL if buyer and seller has no ETH/WETH transfer for the day 
                     , pw.tx_hash AS pos_wash_tx -- will be NULL if it wasn't a wash trading A->B->A 
                     , s.price
                     , s.price_usd
                     , s.nft_address
                     , s.project_name
                     , s.tokenid
              
                     , (CASE WHEN etx.eth_transferred IS NOT NULL AND pw.tx_hash IS NOT NULL THEN 'PoS & Self-Fund Wash' -- highly probably 
                             WHEN etx.eth_transferred IS NULL     AND pw.tx_hash IS NOT NULL THEN 'PoS Wash'             -- very likely wash  
                             WHEN etx.eth_transferred IS NOT NULL AND pw.tx_hash IS NULL     THEN 'Self-Fund Wash'       -- likely wash  
                             WHEN etx.eth_transferred IS NULL     AND pw.tx_hash IS NULL     THEN 'Organic'              -- organic   
                             END) AS sales_type
              
                FROM ethereum.core.ez_nft_sales AS s
                    LEFT JOIN eth_tx AS etx
                        ON s.buyer_address = etx.eth_to_address -- buyer (B) received ETH from...
                        AND s.seller_address = etx.eth_from_address -- seller (A)  
                        AND s.block_timestamp::date = etx.utc_date -- on the same day on the same day of sales 
                  
                    LEFT JOIN pos_wash_tx AS pw
                        ON pw.tx_hash = s.tx_hash
                
                WHERE s.block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
                    AND s.currency_symbol IN ('WETH', 'ETH') -- Filter sales in WETH/ETH
            )
            
            
            SELECT DISTINCT tx_hash, nft_address, tokenid
            FROM labelled_trades
            WHERE sales_type != 'Organic'
        ),
        
        
        nft_sales__wash_trade_filter_off AS (
            SELECT * 
            FROM nft_sales
        ),
        
        
        nft_sales__wash_trade_filter_on AS (
            SELECT * 
            FROM nft_sales
            WHERE tx_hash NOT IN (SELECT tx_hash FROM wash_trades)
        ),
        
        
        daily_trader_stats AS (
            SELECT utc_date
                 , trader
        
                 , (CASE WHEN utc_date BETWEEN CURRENT_DATE - interval '{TimePeriodDays} days'
                                           AND CURRENT_DATE - interval '1 day' 
                                          THEN 'Current Period'
          
                         WHEN utc_date BETWEEN CURRENT_DATE - interval '{TimePeriodDays} days' - interval '{TimePeriodDays} days' 
                                           AND CURRENT_DATE - interval '{TimePeriodDays} days' - interval '1 day'
                                          THEN 'Prior Period'
          
                         WHEN utc_date BETWEEN CURRENT_DATE - interval '1 year' - interval '{TimePeriodDays} days'
                                           AND CURRENT_DATE - interval '1 year' - interval '1 day'
                                          THEN 'Prior Year'
          
                         ELSE '-' END) AS period
          
                 , sum(CASE WHEN tx_type = 'buy' THEN tx_count ELSE 0 END) AS buy_txs
                 , sum(CASE WHEN tx_type = 'buy' THEN volume_usd ELSE 0 END) AS buy_volume_usd
                 , sum(CASE WHEN tx_type = 'buy' THEN volume_eth ELSE 0 END) AS buy_volume_eth
                 , sum(CASE WHEN tx_type = 'sell' THEN tx_count ELSE 0 END) AS sell_txs
                 , sum(CASE WHEN tx_type = 'sell' THEN volume_usd ELSE 0 END) AS sell_volume_usd
                 , sum(CASE WHEN tx_type = 'sell' THEN volume_eth ELSE 0 END) AS sell_volume_eth
                 , sum(tx_count) AS total_txs
                 , sum(volume_usd) AS total_volume_usd
                 , sum(volume_eth) AS total_volume_eth
          
            FROM (
                SELECT block_timestamp::date AS utc_date
                     , buyer_address AS trader
                     , 'buy' AS tx_type
                     , count(*) AS tx_count
                     , sum(price_usd) AS volume_usd
                     , sum(price_eth) AS volume_eth
                FROM nft_sales__wash_trade_filter_{WashTradeFilter}
                GROUP BY 1,2,3
        
                UNION ALL
          
                SELECT block_timestamp::date AS utc_date
                     , seller_address AS trader
                     , 'sell' AS tx_type
                     , count(*) AS tx_count
                     , sum(price_usd) AS volume_usd
                     , sum(price_eth) AS volume_eth
                FROM nft_sales__wash_trade_filter_{WashTradeFilter}
                GROUP BY 1,2,3
            ) AS t
        
            GROUP BY 1,2,3
        ),
        
        
        trader_summary_stats AS (
            SELECT trader
                 , period
                 , sum(total_txs) AS count_trades
                 , sum(total_volume_usd) AS volume_usd
                 , sum(total_volume_eth) AS volume_eth
            FROM daily_trader_stats
            WHERE period IN ('Current Period', 'Prior Period', 'Prior Year')
            GROUP BY 1,2
        ),
        
        
        period_summary_stats AS (
            SELECT period
                 , count(trader) AS wallets
                 , count(CASE WHEN count_trades = 1 THEN trader ELSE NULL END) AS wallets__single_tx
                 , count(CASE WHEN count_trades > 1 THEN trader ELSE NULL END) AS wallets__multiple_tx
          
                 , sum(volume_usd) / 2 AS trading_volume__usd
                 , sum(volume_eth) / 2 AS trading_volume__eth
          
                 , sum(volume_usd) / count(trader) AS volume_per_wallet__usd
                 , sum(volume_eth) / count(trader) AS volume_per_wallet__eth
                 , sum(count_trades) / count(trader) AS transactions_per_wallet
                 , sum(volume_usd) / sum(count_trades) AS volume_per_transaction__usd
                 , sum(volume_eth) / sum(count_trades) AS volume_per_transaction__eth
        
                 , trading_volume__{Currency} AS trading_volume
                 , volume_per_wallet__{Currency} AS volume_per_wallet
                 , volume_per_transaction__{Currency} AS volume_per_transaction
          
            FROM trader_summary_stats
            WHERE count_trades >= 1 
            GROUP BY 1
        ),
        
        
        metric_pivot AS (
            SELECT 'Volume per Wallet' AS metric
                 , (CASE WHEN '{Currency}' = 'ETH' THEN 'Ξ'
                         WHEN '{Currency}' = 'USD' THEN '$' END) AS currency_symbol
                 , avg(CASE WHEN period = 'Current Period' THEN volume_per_wallet ELSE NULL END) AS value__current_period
                 , avg(CASE WHEN period = 'Prior Period' THEN volume_per_wallet ELSE NULL END) AS value__prior_period
                 , avg(CASE WHEN period = 'Prior Year' THEN volume_per_wallet ELSE NULL END) AS value__prior_year
                 , 100.00 * (value__current_period / nullif(value__prior_period, 0) - 1) AS perc_change_vs_prior_period
                 , 100.00 * (value__current_period / nullif(value__prior_year, 0) - 1) AS perc_change_vs_prior_year
            FROM period_summary_stats
            GROUP BY 1,2
        ),
        
        
        unpivot AS (
            SELECT 1 AS _order_
                 , 'Current Period' AS metric
                 , concat(currency_symbol, ' ', to_varchar(value__current_period, '999,999,999,999.0000')) AS value_
            FROM metric_pivot
        
            UNION ALL
          
            SELECT 2 AS _order_
                 , 'Prior Period' AS metric
                 , concat(currency_symbol, ' ', to_varchar(value__prior_period, '999,999,999,999.0000')) AS value_
            FROM metric_pivot
        
            UNION ALL
          
            SELECT 3 AS _order_
                 , 'Prior Year' AS metric
                 , concat(currency_symbol, ' ', to_varchar(value__prior_year, '999,999,999,999.0000')) AS value_
            FROM metric_pivot
        
            UNION ALL
          
            SELECT 4 AS _order_
                 , '% Change vs Prior Period' AS metric
                 , concat(to_varchar(perc_change_vs_prior_period, 'S999,999,999,999.00'), '%') AS value_
            FROM metric_pivot
        
            UNION ALL
          
            SELECT 5 AS _order_
                 , '% Change vs Prior Year' AS metric
                 , concat(to_varchar(perc_change_vs_prior_year, 'S999,999,999,999.00'), '%') AS value_
            FROM metric_pivot
        )
        
        
        SELECT metric AS "Metric"
             , value_ AS "Value"
        FROM unpivot
        ORDER BY _order_
        LIMIT 10000
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

def get_txns_per_wlt_stats(
        TimePeriodDays: int = 365,
        Marketplace: str = "All",
        ContractAddress: str = "",
        WashTradeFilter: str = "On",
        Currency: str = "USD") -> pd.DataFrame:
    """

    :return:
    """
    query = f"""
   WITH
        nft_sales AS (
            SELECT s.tx_hash
                 , s.block_timestamp
                 , s.buyer_address
                 , s.seller_address
                 , s.platform_name
                 , s.nft_address
                 , s.tokenid AS token_id
                 , s.price_usd
                 , s.creator_fee_usd
                 , s.platform_fee_usd
                 , (CASE WHEN s.currency_symbol IN ('ETH','WETH') THEN s.price
                         ELSE s.price_usd / p.price END) AS price_eth
                 , (CASE WHEN s.currency_symbol IN ('ETH','WETH') THEN s.creator_fee
                         ELSE s.creator_fee_usd / p.price END) AS creator_fee_eth
                 , (CASE WHEN s.currency_symbol IN ('ETH','WETH') THEN s.platform_fee
                         ELSE s.platform_fee_usd / p.price END) AS platform_fee_eth
              
            FROM ethereum.core.ez_nft_sales AS s
                LEFT JOIN ethereum.core.fact_hourly_token_prices AS p
                    ON p.hour = date_trunc('hour', s.block_timestamp)
                    AND p.symbol = 'WETH'
        
            WHERE True
          -- Exclude non-collectibles
              AND nft_address NOT IN ( '0xc36442b4a4522e871399cd717abdd847ab11fe88' -- Uniswap V3
                                     , '0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85' -- ENS
                                     , '0x58a3c68e2d3aaf316239c003779f71acb870ee47' -- Curve Finance
                                     )
           -- Filter out weird data points
              AND NOT (creator_fee < 0)
              AND s.price > 0
              AND NOT (currency_symbol = 'ETH' AND currency_address NOT IN ('ETH', '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'))  -- incorrect currency
          
          -- Dashboard filters
              AND block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
              AND (CASE WHEN lower('{Marketplace}') IN ('', '-', 'all', 'combined') THEN True
                        ELSE lower('{Marketplace}') = platform_name END)
              AND (CASE WHEN '{ContractAddress}' = '' THEN True
                        WHEN '{ContractAddress}' = '-' THEN True
                        WHEN '{ContractAddress}' = 'None' THEN True
                        WHEN '{ContractAddress}' = 'All' THEN True
                        WHEN '{ContractAddress}' IS NULL THEN True
                        ELSE lower('{ContractAddress}') = nft_address END)
        ),
        
        
        wash_trades AS (
         -- Query logic forked from:
         --   https://app.flipsidecrypto.com/velocity/queries/19bb605d-ef5b-4d76-b84e-386c93341f12
         --   by pinehearst
          
            WITH 
                
            eth_tx AS ( -- group day, A, B transfers 
                SELECT utc_date
                     , eth_from_address -- A 
                     , eth_to_address   -- B 
                     , sum(eth) as eth_transferred -- ETH/WETH transferred for the day 
                
                FROM (
                    SELECT tx_hash
                         , date_trunc('day', block_timestamp) AS utc_date
                         , eth_from_address -- A
                         , eth_to_address   -- B 
                         , amount AS eth 
                    FROM ethereum.core.ez_eth_transfers
                    WHERE block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
                      AND eth_from_address IN (SELECT DISTINCT buyer_address FROM ethereum.core.ez_nft_sales) -- filter out users that traded NFT 
                      AND tx_hash NOT IN (SELECT DISTINCT tx_hash FROM ethereum.core.ez_nft_sales) -- filter out ETH transfer from buying/selling 
                    
                    UNION 
                    
                    SELECT tx_hash
                         , date_trunc('day', block_timestamp) AS utc_date
                         , from_address AS eth_from_address -- A 
                         , to_address  AS eth_to_address    -- B 
                         , amount AS eth
                    FROM ethereum.core.ez_token_transfers
                    WHERE contract_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                      AND block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
                      AND from_address IN (SELECT DISTINCT buyer_address FROM ethereum.core.ez_nft_sales) -- filter out users that traded NFT 
                      AND tx_hash NOT IN (SELECT DISTINCT tx_hash FROM ethereum.core.ez_nft_sales) -- filter out WETH transfer from buying/selling i.e. back end WETH transfers 
                ) AS tx
                
                GROUP BY 1,2,3 
            ),
            
              
            pos_wash_tx AS (
                SELECT DISTINCT tx_hash
                FROM (
                    SELECT a.block_timestamp
                         , a.platform_name
                         , a.tx_hash
                         , datediff('minute', a.block_timestamp, b.block_timestamp) AS minutes_diff
                         , a.buyer_address AS buyerB -- B 
                         , b.seller_address AS sellerB -- B 
                         , a.seller_address AS sellerA -- A 
                         , b.buyer_address AS buyerA -- A 
                
                    FROM ethereum.core.ez_nft_sales AS a
                        LEFT JOIN ethereum.core.ez_nft_sales AS b 
                            ON a.nft_address = b.nft_address 
                            AND a.tokenid = b.tokenid 
                            AND a.buyer_address = b.seller_address -- buyer was a seller of the nft  	
                            AND a.seller_address = b.buyer_address -- seller was a buyer of the nft 
              
                    WHERE a.block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
                ) AS tx
                WHERE sellerB IS NOT NULL AND buyerA IS NOT NULL 
            ),
            
              
            labelled_trades AS (
                SELECT s.tx_hash
                     , s.block_timestamp
                     , s.platform_name
                     , s.buyer_address  -- B 
                     , s.seller_address -- A 
                     , etx.eth_transferred -- will be NULL if buyer and seller has no ETH/WETH transfer for the day 
                     , pw.tx_hash AS pos_wash_tx -- will be NULL if it wasn't a wash trading A->B->A 
                     , s.price
                     , s.price_usd
                     , s.nft_address
                     , s.project_name
                     , s.tokenid
              
                     , (CASE WHEN etx.eth_transferred IS NOT NULL AND pw.tx_hash IS NOT NULL THEN 'PoS & Self-Fund Wash' -- highly probably 
                             WHEN etx.eth_transferred IS NULL     AND pw.tx_hash IS NOT NULL THEN 'PoS Wash'             -- very likely wash  
                             WHEN etx.eth_transferred IS NOT NULL AND pw.tx_hash IS NULL     THEN 'Self-Fund Wash'       -- likely wash  
                             WHEN etx.eth_transferred IS NULL     AND pw.tx_hash IS NULL     THEN 'Organic'              -- organic   
                             END) AS sales_type
              
                FROM ethereum.core.ez_nft_sales AS s
                    LEFT JOIN eth_tx AS etx
                        ON s.buyer_address = etx.eth_to_address -- buyer (B) received ETH from...
                        AND s.seller_address = etx.eth_from_address -- seller (A)  
                        AND s.block_timestamp::date = etx.utc_date -- on the same day on the same day of sales 
                  
                    LEFT JOIN pos_wash_tx AS pw
                        ON pw.tx_hash = s.tx_hash
                
                WHERE s.block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
                    AND s.currency_symbol IN ('WETH', 'ETH') -- Filter sales in WETH/ETH
            )
            
            
            SELECT DISTINCT tx_hash, nft_address, tokenid
            FROM labelled_trades
            WHERE sales_type != 'Organic'
        ),
        
        
        nft_sales__wash_trade_filter_off AS (
            SELECT * 
            FROM nft_sales
        ),
        
        
        nft_sales__wash_trade_filter_on AS (
            SELECT * 
            FROM nft_sales
            WHERE tx_hash NOT IN (SELECT tx_hash FROM wash_trades)
        ),
        
        
        daily_trader_stats AS (
            SELECT utc_date
                 , trader
        
                 , (CASE WHEN utc_date BETWEEN CURRENT_DATE - interval '{TimePeriodDays} days'
                                           AND CURRENT_DATE - interval '1 day' 
                                          THEN 'Current Period'
          
                         WHEN utc_date BETWEEN CURRENT_DATE - interval '{TimePeriodDays} days' - interval '{TimePeriodDays} days' 
                                           AND CURRENT_DATE - interval '{TimePeriodDays} days' - interval '1 day'
                                          THEN 'Prior Period'
          
                         WHEN utc_date BETWEEN CURRENT_DATE - interval '1 year' - interval '{TimePeriodDays} days'
                                           AND CURRENT_DATE - interval '1 year' - interval '1 day'
                                          THEN 'Prior Year'
          
                         ELSE '-' END) AS period
          
                 , sum(CASE WHEN tx_type = 'buy' THEN tx_count ELSE 0 END) AS buy_txs
                 , sum(CASE WHEN tx_type = 'buy' THEN volume_usd ELSE 0 END) AS buy_volume_usd
                 , sum(CASE WHEN tx_type = 'buy' THEN volume_eth ELSE 0 END) AS buy_volume_eth
                 , sum(CASE WHEN tx_type = 'sell' THEN tx_count ELSE 0 END) AS sell_txs
                 , sum(CASE WHEN tx_type = 'sell' THEN volume_usd ELSE 0 END) AS sell_volume_usd
                 , sum(CASE WHEN tx_type = 'sell' THEN volume_eth ELSE 0 END) AS sell_volume_eth
                 , sum(tx_count) AS total_txs
                 , sum(volume_usd) AS total_volume_usd
                 , sum(volume_eth) AS total_volume_eth
          
            FROM (
                SELECT block_timestamp::date AS utc_date
                     , buyer_address AS trader
                     , 'buy' AS tx_type
                     , count(*) AS tx_count
                     , sum(price_usd) AS volume_usd
                     , sum(price_eth) AS volume_eth
                FROM nft_sales__wash_trade_filter_{WashTradeFilter}
                GROUP BY 1,2,3
        
                UNION ALL
          
                SELECT block_timestamp::date AS utc_date
                     , seller_address AS trader
                     , 'sell' AS tx_type
                     , count(*) AS tx_count
                     , sum(price_usd) AS volume_usd
                     , sum(price_eth) AS volume_eth
                FROM nft_sales__wash_trade_filter_{WashTradeFilter}
                GROUP BY 1,2,3
            ) AS t
        
            GROUP BY 1,2,3
        ),
        
        
        trader_summary_stats AS (
            SELECT trader
                 , period
                 , sum(total_txs) AS count_trades
                 , sum(total_volume_usd) AS volume_usd
                 , sum(total_volume_eth) AS volume_eth
            FROM daily_trader_stats
            WHERE period IN ('Current Period', 'Prior Period', 'Prior Year')
            GROUP BY 1,2
        ),
        
        
        period_summary_stats AS (
            SELECT period
                 , count(trader) AS wallets
                 , count(CASE WHEN count_trades = 1 THEN trader ELSE NULL END) AS wallets__single_tx
                 , count(CASE WHEN count_trades > 1 THEN trader ELSE NULL END) AS wallets__multiple_tx
          
                 , sum(volume_usd) / 2 AS trading_volume__usd
                 , sum(volume_eth) / 2 AS trading_volume__eth
          
                 , sum(volume_usd) / count(trader) AS volume_per_wallet__usd
                 , sum(volume_eth) / count(trader) AS volume_per_wallet__eth
                 , sum(count_trades) / count(trader) AS transactions_per_wallet
                 , sum(volume_usd) / sum(count_trades) AS volume_per_transaction__usd
                 , sum(volume_eth) / sum(count_trades) AS volume_per_transaction__eth
        
                 , trading_volume__{Currency} AS trading_volume
                 , volume_per_wallet__{Currency} AS volume_per_wallet
                 , volume_per_transaction__{Currency} AS volume_per_transaction
          
            FROM trader_summary_stats
            WHERE count_trades >= 1 
            GROUP BY 1
        ),
        
        
        metric_pivot AS (
            SELECT 'Transactions per Wallet' AS metric
                 , avg(CASE WHEN period = 'Current Period' THEN transactions_per_wallet ELSE NULL END) AS value__current_period
                 , avg(CASE WHEN period = 'Prior Period' THEN transactions_per_wallet ELSE NULL END) AS value__prior_period
                 , avg(CASE WHEN period = 'Prior Year' THEN transactions_per_wallet ELSE NULL END) AS value__prior_year
                 , 100.00 * (value__current_period / nullif(value__prior_period, 0) - 1) AS perc_change_vs_prior_period
                 , 100.00 * (value__current_period / nullif(value__prior_year, 0) - 1) AS perc_change_vs_prior_year
            FROM period_summary_stats
            GROUP BY 1
        ),
        
        
        unpivot AS (
            SELECT 1 AS _order_
                 , 'Current Period' AS metric
                 , to_varchar(value__current_period, '999,999,999,999.0') AS value_
            FROM metric_pivot
        
            UNION ALL
          
            SELECT 2 AS _order_
                 , 'Prior Period' AS metric
                 , to_varchar(value__prior_period, '999,999,999,999.0') AS value_
            FROM metric_pivot
        
            UNION ALL
          
            SELECT 3 AS _order_
                 , 'Prior Year' AS metric
                 , to_varchar(value__prior_year, '999,999,999,999.0') AS value_
            FROM metric_pivot
        
            UNION ALL
          
            SELECT 4 AS _order_
                 , '% Change vs Prior Period' AS metric
                 , concat(to_varchar(perc_change_vs_prior_period, 'S999,999,999,999.00'), '%') AS value_
            FROM metric_pivot
        
            UNION ALL
          
            SELECT 5 AS _order_
                 , '% Change vs Prior Year' AS metric
                 , concat(to_varchar(perc_change_vs_prior_year, 'S999,999,999,999.00'), '%') AS value_
            FROM metric_pivot
        )
        
        
        SELECT metric AS "Metric"
             , value_ AS "Value"
        FROM unpivot
        ORDER BY _order_
        LIMIT 10000
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


def get_vol_per_txn_stats(
        TimePeriodDays: int = 365,
        Marketplace: str = "All",
        ContractAddress: str = "",
        WashTradeFilter: str = "On",
        Currency: str = "USD") -> pd.DataFrame:
    """

    :return:
    """
    query = f"""
    WITH
        nft_sales AS (
            SELECT s.tx_hash
                 , s.block_timestamp
                 , s.buyer_address
                 , s.seller_address
                 , s.platform_name
                 , s.nft_address
                 , s.tokenid AS token_id
                 , s.price_usd
                 , s.creator_fee_usd
                 , s.platform_fee_usd
                 , (CASE WHEN s.currency_symbol IN ('ETH','WETH') THEN s.price
                         ELSE s.price_usd / p.price END) AS price_eth
                 , (CASE WHEN s.currency_symbol IN ('ETH','WETH') THEN s.creator_fee
                         ELSE s.creator_fee_usd / p.price END) AS creator_fee_eth
                 , (CASE WHEN s.currency_symbol IN ('ETH','WETH') THEN s.platform_fee
                         ELSE s.platform_fee_usd / p.price END) AS platform_fee_eth
              
            FROM ethereum.core.ez_nft_sales AS s
                LEFT JOIN ethereum.core.fact_hourly_token_prices AS p
                    ON p.hour = date_trunc('hour', s.block_timestamp)
                    AND p.symbol = 'WETH'
        
            WHERE True
          -- Exclude non-collectibles
              AND nft_address NOT IN ( '0xc36442b4a4522e871399cd717abdd847ab11fe88' -- Uniswap V3
                                     , '0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85' -- ENS
                                     , '0x58a3c68e2d3aaf316239c003779f71acb870ee47' -- Curve Finance
                                     )
           -- Filter out weird data points
              AND NOT (creator_fee < 0)
              AND s.price > 0
              AND NOT (currency_symbol = 'ETH' AND currency_address NOT IN ('ETH', '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'))  -- incorrect currency
          
          -- Dashboard filters
              AND block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
              AND (CASE WHEN lower('{Marketplace}') IN ('', '-', 'all', 'combined') THEN True
                        ELSE lower('{Marketplace}') = platform_name END)
              AND (CASE WHEN '{ContractAddress}' = '' THEN True
                        WHEN '{ContractAddress}' = '-' THEN True
                        WHEN '{ContractAddress}' = 'None' THEN True
                        WHEN '{ContractAddress}' = 'All' THEN True
                        WHEN '{ContractAddress}' IS NULL THEN True
                        ELSE lower('{ContractAddress}') = nft_address END)
        ),
        
        
        wash_trades AS (
         -- Query logic forked from:
         --   https://app.flipsidecrypto.com/velocity/queries/19bb605d-ef5b-4d76-b84e-386c93341f12
         --   by pinehearst
          
            WITH 
                
            eth_tx AS ( -- group day, A, B transfers 
                SELECT utc_date
                     , eth_from_address -- A 
                     , eth_to_address   -- B 
                     , sum(eth) as eth_transferred -- ETH/WETH transferred for the day 
                
                FROM (
                    SELECT tx_hash
                         , date_trunc('day', block_timestamp) AS utc_date
                         , eth_from_address -- A
                         , eth_to_address   -- B 
                         , amount AS eth 
                    FROM ethereum.core.ez_eth_transfers
                    WHERE block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
                      AND eth_from_address IN (SELECT DISTINCT buyer_address FROM ethereum.core.ez_nft_sales) -- filter out users that traded NFT 
                      AND tx_hash NOT IN (SELECT DISTINCT tx_hash FROM ethereum.core.ez_nft_sales) -- filter out ETH transfer from buying/selling 
                    
                    UNION 
                    
                    SELECT tx_hash
                         , date_trunc('day', block_timestamp) AS utc_date
                         , from_address AS eth_from_address -- A 
                         , to_address  AS eth_to_address    -- B 
                         , amount AS eth
                    FROM ethereum.core.ez_token_transfers
                    WHERE contract_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                      AND block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
                      AND from_address IN (SELECT DISTINCT buyer_address FROM ethereum.core.ez_nft_sales) -- filter out users that traded NFT 
                      AND tx_hash NOT IN (SELECT DISTINCT tx_hash FROM ethereum.core.ez_nft_sales) -- filter out WETH transfer from buying/selling i.e. back end WETH transfers 
                ) AS tx
                
                GROUP BY 1,2,3 
            ),
            
              
            pos_wash_tx AS (
                SELECT DISTINCT tx_hash
                FROM (
                    SELECT a.block_timestamp
                         , a.platform_name
                         , a.tx_hash
                         , datediff('minute', a.block_timestamp, b.block_timestamp) AS minutes_diff
                         , a.buyer_address AS buyerB -- B 
                         , b.seller_address AS sellerB -- B 
                         , a.seller_address AS sellerA -- A 
                         , b.buyer_address AS buyerA -- A 
                
                    FROM ethereum.core.ez_nft_sales AS a
                        LEFT JOIN ethereum.core.ez_nft_sales AS b 
                            ON a.nft_address = b.nft_address 
                            AND a.tokenid = b.tokenid 
                            AND a.buyer_address = b.seller_address -- buyer was a seller of the nft  	
                            AND a.seller_address = b.buyer_address -- seller was a buyer of the nft 
              
                    WHERE a.block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
                ) AS tx
                WHERE sellerB IS NOT NULL AND buyerA IS NOT NULL 
            ),
            
              
            labelled_trades AS (
                SELECT s.tx_hash
                     , s.block_timestamp
                     , s.platform_name
                     , s.buyer_address  -- B 
                     , s.seller_address -- A 
                     , etx.eth_transferred -- will be NULL if buyer and seller has no ETH/WETH transfer for the day 
                     , pw.tx_hash AS pos_wash_tx -- will be NULL if it wasn't a wash trading A->B->A 
                     , s.price
                     , s.price_usd
                     , s.nft_address
                     , s.project_name
                     , s.tokenid
              
                     , (CASE WHEN etx.eth_transferred IS NOT NULL AND pw.tx_hash IS NOT NULL THEN 'PoS & Self-Fund Wash' -- highly probably 
                             WHEN etx.eth_transferred IS NULL     AND pw.tx_hash IS NOT NULL THEN 'PoS Wash'             -- very likely wash  
                             WHEN etx.eth_transferred IS NOT NULL AND pw.tx_hash IS NULL     THEN 'Self-Fund Wash'       -- likely wash  
                             WHEN etx.eth_transferred IS NULL     AND pw.tx_hash IS NULL     THEN 'Organic'              -- organic   
                             END) AS sales_type
              
                FROM ethereum.core.ez_nft_sales AS s
                    LEFT JOIN eth_tx AS etx
                        ON s.buyer_address = etx.eth_to_address -- buyer (B) received ETH from...
                        AND s.seller_address = etx.eth_from_address -- seller (A)  
                        AND s.block_timestamp::date = etx.utc_date -- on the same day on the same day of sales 
                  
                    LEFT JOIN pos_wash_tx AS pw
                        ON pw.tx_hash = s.tx_hash
                
                WHERE s.block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
                    AND s.currency_symbol IN ('WETH', 'ETH') -- Filter sales in WETH/ETH
            )
            
            
            SELECT DISTINCT tx_hash, nft_address, tokenid
            FROM labelled_trades
            WHERE sales_type != 'Organic'
        ),
        
        
        nft_sales__wash_trade_filter_off AS (
            SELECT * 
            FROM nft_sales
        ),
        
        
        nft_sales__wash_trade_filter_on AS (
            SELECT * 
            FROM nft_sales
            WHERE tx_hash NOT IN (SELECT tx_hash FROM wash_trades)
        ),
        
        
        daily_trader_stats AS (
            SELECT utc_date
                 , trader
        
                 , (CASE WHEN utc_date BETWEEN CURRENT_DATE - interval '{TimePeriodDays} days'
                                           AND CURRENT_DATE - interval '1 day' 
                                          THEN 'Current Period'
          
                         WHEN utc_date BETWEEN CURRENT_DATE - interval '{TimePeriodDays} days' - interval '{TimePeriodDays} days' 
                                           AND CURRENT_DATE - interval '{TimePeriodDays} days' - interval '1 day'
                                          THEN 'Prior Period'
          
                         WHEN utc_date BETWEEN CURRENT_DATE - interval '1 year' - interval '{TimePeriodDays} days'
                                           AND CURRENT_DATE - interval '1 year' - interval '1 day'
                                          THEN 'Prior Year'
          
                         ELSE '-' END) AS period
          
                 , sum(CASE WHEN tx_type = 'buy' THEN tx_count ELSE 0 END) AS buy_txs
                 , sum(CASE WHEN tx_type = 'buy' THEN volume_usd ELSE 0 END) AS buy_volume_usd
                 , sum(CASE WHEN tx_type = 'buy' THEN volume_eth ELSE 0 END) AS buy_volume_eth
                 , sum(CASE WHEN tx_type = 'sell' THEN tx_count ELSE 0 END) AS sell_txs
                 , sum(CASE WHEN tx_type = 'sell' THEN volume_usd ELSE 0 END) AS sell_volume_usd
                 , sum(CASE WHEN tx_type = 'sell' THEN volume_eth ELSE 0 END) AS sell_volume_eth
                 , sum(tx_count) AS total_txs
                 , sum(volume_usd) AS total_volume_usd
                 , sum(volume_eth) AS total_volume_eth
          
            FROM (
                SELECT block_timestamp::date AS utc_date
                     , buyer_address AS trader
                     , 'buy' AS tx_type
                     , count(*) AS tx_count
                     , sum(price_usd) AS volume_usd
                     , sum(price_eth) AS volume_eth
                FROM nft_sales__wash_trade_filter_{WashTradeFilter}
                GROUP BY 1,2,3
        
                UNION ALL
          
                SELECT block_timestamp::date AS utc_date
                     , seller_address AS trader
                     , 'sell' AS tx_type
                     , count(*) AS tx_count
                     , sum(price_usd) AS volume_usd
                     , sum(price_eth) AS volume_eth
                FROM nft_sales__wash_trade_filter_{WashTradeFilter}
                GROUP BY 1,2,3
            ) AS t
        
            GROUP BY 1,2,3
        ),
        
        
        trader_summary_stats AS (
            SELECT trader
                 , period
                 , sum(total_txs) AS count_trades
                 , sum(total_volume_usd) AS volume_usd
                 , sum(total_volume_eth) AS volume_eth
            FROM daily_trader_stats
            WHERE period IN ('Current Period', 'Prior Period', 'Prior Year')
            GROUP BY 1,2
        ),
        
        
        period_summary_stats AS (
            SELECT period
                 , count(trader) AS wallets
                 , count(CASE WHEN count_trades = 1 THEN trader ELSE NULL END) AS wallets__single_tx
                 , count(CASE WHEN count_trades > 1 THEN trader ELSE NULL END) AS wallets__multiple_tx
          
                 , sum(volume_usd) / 2 AS trading_volume__usd
                 , sum(volume_eth) / 2 AS trading_volume__eth
          
                 , sum(volume_usd) / count(trader) AS volume_per_wallet__usd
                 , sum(volume_eth) / count(trader) AS volume_per_wallet__eth
                 , sum(count_trades) / count(trader) AS transactions_per_wallet
                 , sum(volume_usd) / sum(count_trades) AS volume_per_transaction__usd
                 , sum(volume_eth) / sum(count_trades) AS volume_per_transaction__eth
        
                 , trading_volume__{Currency} AS trading_volume
                 , volume_per_wallet__{Currency} AS volume_per_wallet
                 , volume_per_transaction__{Currency} AS volume_per_transaction
          
            FROM trader_summary_stats
            WHERE count_trades >= 1 
            GROUP BY 1
        ),
        
        
        metric_pivot AS (
            SELECT 'Volume per TX' AS metric
                 , (CASE WHEN '{Currency}' = 'ETH' THEN 'Ξ'
                         WHEN '{Currency}' = 'USD' THEN '$' END) AS currency_symbol
                 , avg(CASE WHEN period = 'Current Period' THEN volume_per_transaction ELSE NULL END) AS value__current_period
                 , avg(CASE WHEN period = 'Prior Period' THEN volume_per_transaction ELSE NULL END) AS value__prior_period
                 , avg(CASE WHEN period = 'Prior Year' THEN volume_per_transaction ELSE NULL END) AS value__prior_year
                 , 100.00 * (value__current_period / nullif(value__prior_period, 0) - 1) AS perc_change_vs_prior_period
                 , 100.00 * (value__current_period / nullif(value__prior_year, 0) - 1) AS perc_change_vs_prior_year
            FROM period_summary_stats
            GROUP BY 1,2
        ),
        
        
        unpivot AS (
            SELECT 1 AS _order_
                 , 'Current Period' AS metric
                 , concat(currency_symbol, ' ', to_varchar(value__current_period, '999,999,999,999.0000')) AS value_
            FROM metric_pivot
        
            UNION ALL
          
            SELECT 2 AS _order_
                 , 'Prior Period' AS metric
                 , concat(currency_symbol, ' ', to_varchar(value__prior_period, '999,999,999,999.0000')) AS value_
            FROM metric_pivot
        
            UNION ALL
          
            SELECT 3 AS _order_
                 , 'Prior Year' AS metric
                 , concat(currency_symbol, ' ', to_varchar(value__prior_year, '999,999,999,999.0000')) AS value_
            FROM metric_pivot
        
            UNION ALL
          
            SELECT 4 AS _order_
                 , '% Change vs Prior Period' AS metric
                 , concat(to_varchar(perc_change_vs_prior_period, 'S999,999,999,999.00'), '%') AS value_
            FROM metric_pivot
        
            UNION ALL
          
            SELECT 5 AS _order_
                 , '% Change vs Prior Year' AS metric
                 , concat(to_varchar(perc_change_vs_prior_year, 'S999,999,999,999.00'), '%') AS value_
            FROM metric_pivot
        )
        
        
        SELECT metric AS "Metric"
             , value_ AS "Value"
        FROM unpivot
        ORDER BY _order_
        LIMIT 10000
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


def get_creator_fees_stats(
        TimePeriodDays: int = 365,
        Marketplace: str = "All",
        ContractAddress: str = "",
        WashTradeFilter: str = "On",
        Currency: str = "USD") -> pd.DataFrame:
    """

    :return:
    """
    query = f"""
    WITH
        nft_sales AS (
            SELECT s.tx_hash
                 , s.block_timestamp
                 , s.buyer_address
                 , s.seller_address
                 , s.platform_name
                 , s.nft_address
                 , s.tokenid AS token_id
                 , s.price_usd
                 , s.creator_fee_usd
                 , s.platform_fee_usd
                 , (CASE WHEN s.currency_symbol IN ('ETH','WETH') THEN s.price
                         ELSE s.price_usd / p.price END) AS price_eth
                 , (CASE WHEN s.currency_symbol IN ('ETH','WETH') THEN s.creator_fee
                         ELSE s.creator_fee_usd / p.price END) AS creator_fee_eth
                 , (CASE WHEN s.currency_symbol IN ('ETH','WETH') THEN s.platform_fee
                         ELSE s.platform_fee_usd / p.price END) AS platform_fee_eth
              
            FROM ethereum.core.ez_nft_sales AS s
                LEFT JOIN ethereum.core.fact_hourly_token_prices AS p
                    ON p.hour = date_trunc('hour', s.block_timestamp)
                    AND p.symbol = 'WETH'
        
            WHERE True
          -- Exclude non-collectibles
              AND nft_address NOT IN ( '0xc36442b4a4522e871399cd717abdd847ab11fe88' -- Uniswap V3
                                     , '0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85' -- ENS
                                     , '0x58a3c68e2d3aaf316239c003779f71acb870ee47' -- Curve Finance
                                     )
           -- Filter out weird data points
              AND NOT (creator_fee < 0)
              AND s.price > 0
              AND NOT (currency_symbol = 'ETH' AND currency_address NOT IN ('ETH', '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'))  -- incorrect currency
          
          -- Dashboard filters
              AND block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
              AND (CASE WHEN lower('{Marketplace}') IN ('', '-', 'all', 'combined') THEN True
                        ELSE lower('{Marketplace}') = platform_name END)
              AND (CASE WHEN '{ContractAddress}' = '' THEN True
                        WHEN '{ContractAddress}' = '-' THEN True
                        WHEN '{ContractAddress}' = 'None' THEN True
                        WHEN '{ContractAddress}' = 'All' THEN True
                        WHEN '{ContractAddress}' IS NULL THEN True
                        ELSE lower('{ContractAddress}') = nft_address END)
        ),
        
        
        wash_trades AS (
         -- Query logic forked from:
         --   https://app.flipsidecrypto.com/velocity/queries/19bb605d-ef5b-4d76-b84e-386c93341f12
         --   by pinehearst
          
            WITH 
                
            eth_tx AS ( -- group day, A, B transfers 
                SELECT utc_date
                     , eth_from_address -- A 
                     , eth_to_address   -- B 
                     , sum(eth) as eth_transferred -- ETH/WETH transferred for the day 
                
                FROM (
                    SELECT tx_hash
                         , date_trunc('day', block_timestamp) AS utc_date
                         , eth_from_address -- A
                         , eth_to_address   -- B 
                         , amount AS eth 
                    FROM ethereum.core.ez_eth_transfers
                    WHERE block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
                      AND eth_from_address IN (SELECT DISTINCT buyer_address FROM ethereum.core.ez_nft_sales) -- filter out users that traded NFT 
                      AND tx_hash NOT IN (SELECT DISTINCT tx_hash FROM ethereum.core.ez_nft_sales) -- filter out ETH transfer from buying/selling 
                    
                    UNION 
                    
                    SELECT tx_hash
                         , date_trunc('day', block_timestamp) AS utc_date
                         , from_address AS eth_from_address -- A 
                         , to_address  AS eth_to_address    -- B 
                         , amount AS eth
                    FROM ethereum.core.ez_token_transfers
                    WHERE contract_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                      AND block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
                      AND from_address IN (SELECT DISTINCT buyer_address FROM ethereum.core.ez_nft_sales) -- filter out users that traded NFT 
                      AND tx_hash NOT IN (SELECT DISTINCT tx_hash FROM ethereum.core.ez_nft_sales) -- filter out WETH transfer from buying/selling i.e. back end WETH transfers 
                ) AS tx
                
                GROUP BY 1,2,3 
            ),
            
              
            pos_wash_tx AS (
                SELECT DISTINCT tx_hash
                FROM (
                    SELECT a.block_timestamp
                         , a.platform_name
                         , a.tx_hash
                         , datediff('minute', a.block_timestamp, b.block_timestamp) AS minutes_diff
                         , a.buyer_address AS buyerB -- B 
                         , b.seller_address AS sellerB -- B 
                         , a.seller_address AS sellerA -- A 
                         , b.buyer_address AS buyerA -- A 
                
                    FROM ethereum.core.ez_nft_sales AS a
                        LEFT JOIN ethereum.core.ez_nft_sales AS b 
                            ON a.nft_address = b.nft_address 
                            AND a.tokenid = b.tokenid 
                            AND a.buyer_address = b.seller_address -- buyer was a seller of the nft  	
                            AND a.seller_address = b.buyer_address -- seller was a buyer of the nft 
              
                    WHERE a.block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
                ) AS tx
                WHERE sellerB IS NOT NULL AND buyerA IS NOT NULL 
            ),
            
              
            labelled_trades AS (
                SELECT s.tx_hash
                     , s.block_timestamp
                     , s.platform_name
                     , s.buyer_address  -- B 
                     , s.seller_address -- A 
                     , etx.eth_transferred -- will be NULL if buyer and seller has no ETH/WETH transfer for the day 
                     , pw.tx_hash AS pos_wash_tx -- will be NULL if it wasn't a wash trading A->B->A 
                     , s.price
                     , s.price_usd
                     , s.nft_address
                     , s.project_name
                     , s.tokenid
              
                     , (CASE WHEN etx.eth_transferred IS NOT NULL AND pw.tx_hash IS NOT NULL THEN 'PoS & Self-Fund Wash' -- highly probably 
                             WHEN etx.eth_transferred IS NULL     AND pw.tx_hash IS NOT NULL THEN 'PoS Wash'             -- very likely wash  
                             WHEN etx.eth_transferred IS NOT NULL AND pw.tx_hash IS NULL     THEN 'Self-Fund Wash'       -- likely wash  
                             WHEN etx.eth_transferred IS NULL     AND pw.tx_hash IS NULL     THEN 'Organic'              -- organic   
                             END) AS sales_type
              
                FROM ethereum.core.ez_nft_sales AS s
                    LEFT JOIN eth_tx AS etx
                        ON s.buyer_address = etx.eth_to_address -- buyer (B) received ETH from...
                        AND s.seller_address = etx.eth_from_address -- seller (A)  
                        AND s.block_timestamp::date = etx.utc_date -- on the same day on the same day of sales 
                  
                    LEFT JOIN pos_wash_tx AS pw
                        ON pw.tx_hash = s.tx_hash
                
                WHERE s.block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
                    AND s.currency_symbol IN ('WETH', 'ETH') -- Filter sales in WETH/ETH
            )
            
            
            SELECT DISTINCT tx_hash, nft_address, tokenid
            FROM labelled_trades
            WHERE sales_type != 'Organic'
        ),
        
        
        nft_sales__wash_trade_filter_off AS (
            SELECT * 
            FROM nft_sales
        ),
        
        
        nft_sales__wash_trade_filter_on AS (
            SELECT * 
            FROM nft_sales
            WHERE tx_hash NOT IN (SELECT tx_hash FROM wash_trades)
        ),
        
        
        daily_trader_stats AS (
            SELECT utc_date
                 , trader
        
                 , (CASE WHEN utc_date BETWEEN CURRENT_DATE - interval '{TimePeriodDays} days'
                                           AND CURRENT_DATE - interval '1 day' 
                                          THEN 'Current Period'
          
                         WHEN utc_date BETWEEN CURRENT_DATE - interval '{TimePeriodDays} days' - interval '{TimePeriodDays} days' 
                                           AND CURRENT_DATE - interval '{TimePeriodDays} days' - interval '1 day'
                                          THEN 'Prior Period'
          
                         WHEN utc_date BETWEEN CURRENT_DATE - interval '1 year' - interval '{TimePeriodDays} days'
                                           AND CURRENT_DATE - interval '1 year' - interval '1 day'
                                          THEN 'Prior Year'
          
                         ELSE '-' END) AS period
          
                 , sum(CASE WHEN tx_type = 'buy' THEN tx_count ELSE 0 END) AS buy_txs
                 , sum(CASE WHEN tx_type = 'buy' THEN volume_usd ELSE 0 END) AS buy_volume_usd
                 , sum(CASE WHEN tx_type = 'buy' THEN volume_eth ELSE 0 END) AS buy_volume_eth
                 , sum(CASE WHEN tx_type = 'sell' THEN tx_count ELSE 0 END) AS sell_txs
                 , sum(CASE WHEN tx_type = 'sell' THEN volume_usd ELSE 0 END) AS sell_volume_usd
                 , sum(CASE WHEN tx_type = 'sell' THEN volume_eth ELSE 0 END) AS sell_volume_eth
                 , sum(tx_count) AS total_txs
                 , sum(volume_usd) AS total_volume_usd
                 , sum(volume_eth) AS total_volume_eth
                 , sum(creator_fees_usd) AS total_creator_fees_usd
                 , sum(creator_fees_eth) AS total_creator_fees_eth
                 , sum(platform_fees_usd) AS total_platform_fees_usd
                 , sum(platform_fees_eth) AS total_platform_fees_eth
          
            FROM (
                SELECT block_timestamp::date AS utc_date
                     , buyer_address AS trader
                     , 'buy' AS tx_type
                     , count(*) AS tx_count
                     , sum(price_usd) AS volume_usd
                     , sum(price_eth) AS volume_eth
                     , sum(0) AS creator_fees_usd
                     , sum(0) AS creator_fees_eth
                     , sum(0) AS platform_fees_usd
                     , sum(0) AS platform_fees_eth
                FROM nft_sales__wash_trade_filter_{WashTradeFilter}
                GROUP BY 1,2,3
        
                UNION ALL
          
                SELECT block_timestamp::date AS utc_date
                     , seller_address AS trader
                     , 'sell' AS tx_type
                     , count(*) AS tx_count
                     , sum(price_usd) AS volume_usd
                     , sum(price_eth) AS volume_eth
                     , sum(creator_fee_usd) AS creator_fees_usd
                     , sum(creator_fee_eth) AS creator_fees_eth
                     , sum(platform_fee_usd) AS platform_fees_usd
                     , sum(platform_fee_eth) AS platform_fees_eth
                FROM nft_sales__wash_trade_filter_{WashTradeFilter}
                GROUP BY 1,2,3
            ) AS t
        
            GROUP BY 1,2,3
        ),
        
        
        trader_summary_stats AS (
            SELECT trader
                 , period
                 , sum(total_txs) AS count_trades
                 , sum(total_volume_usd) AS volume_usd
                 , sum(total_volume_eth) AS volume_eth
                 , sum(total_creator_fees_usd) AS creator_fees_usd
                 , sum(total_creator_fees_eth) AS creator_fees_eth
                 , sum(total_platform_fees_usd) AS platform_fees_usd
                 , sum(total_platform_fees_eth) AS platform_fees_eth
            FROM daily_trader_stats
            WHERE period IN ('Current Period', 'Prior Period', 'Prior Year')
            GROUP BY 1,2
        ),
        
        
        period_summary_stats AS (
            SELECT period
                 , count(trader) AS wallets
                 , count(CASE WHEN count_trades = 1 THEN trader ELSE NULL END) AS wallets__single_tx
                 , count(CASE WHEN count_trades > 1 THEN trader ELSE NULL END) AS wallets__multiple_tx
          
                 , sum(volume_usd) / 2 AS trading_volume__usd
                 , sum(volume_eth) / 2 AS trading_volume__eth
          
                 , sum(volume_usd) / count(trader) AS volume_per_wallet__usd
                 , sum(volume_eth) / count(trader) AS volume_per_wallet__eth
                 , sum(count_trades) / count(trader) AS transactions_per_wallet
                 , sum(volume_usd) / sum(count_trades) AS volume_per_transaction__usd
                 , sum(volume_eth) / sum(count_trades) AS volume_per_transaction__eth
        
                 , sum(creator_fees_usd) AS creator_fees__usd
                 , sum(creator_fees_eth) AS creator_fees__eth
                 , sum(platform_fees_usd) AS platform_fees__usd
                 , sum(platform_fees_eth) AS platform_fees__eth
        
                 , trading_volume__{Currency} AS trading_volume
                 , volume_per_wallet__{Currency} AS volume_per_wallet
                 , volume_per_transaction__{Currency} AS volume_per_transaction
                 , creator_fees__{Currency} AS creator_fees
                 , platform_fees__{Currency} AS platform_fees
          
            FROM trader_summary_stats
            WHERE count_trades >= 1 
            GROUP BY 1
        ),
        
        
        metric_pivot AS (
            SELECT 'Creator Fees' AS metric
                 , (CASE WHEN '{Currency}' = 'ETH' THEN 'Ξ'
                         WHEN '{Currency}' = 'USD' THEN '$' END) AS currency_symbol
                 , avg(CASE WHEN period = 'Current Period' THEN creator_fees ELSE NULL END) AS value__current_period
                 , avg(CASE WHEN period = 'Prior Period' THEN creator_fees ELSE NULL END) AS value__prior_period
                 , avg(CASE WHEN period = 'Prior Year' THEN creator_fees ELSE NULL END) AS value__prior_year
                 , 100.00 * (value__current_period / nullif(value__prior_period, 0) - 1) AS perc_change_vs_prior_period
                 , 100.00 * (value__current_period / nullif(value__prior_year, 0) - 1) AS perc_change_vs_prior_year
            FROM period_summary_stats
            GROUP BY 1,2
        ),
        
        
        unpivot AS (
            SELECT 1 AS _order_
                 , 'Current Period' AS metric
                 , concat(currency_symbol, ' ', to_varchar(value__current_period, '999,999,999,999.0000')) AS value_
            FROM metric_pivot
        
            UNION ALL
          
            SELECT 2 AS _order_
                 , 'Prior Period' AS metric
                 , concat(currency_symbol, ' ', to_varchar(value__prior_period, '999,999,999,999.0000')) AS value_
            FROM metric_pivot
        
            UNION ALL
          
            SELECT 3 AS _order_
                 , 'Prior Year' AS metric
                 , concat(currency_symbol, ' ', to_varchar(value__prior_year, '999,999,999,999.0000')) AS value_
            FROM metric_pivot
        
            UNION ALL
          
            SELECT 4 AS _order_
                 , '% Change vs Prior Period' AS metric
                 , concat(to_varchar(perc_change_vs_prior_period, 'S999,999,999,999.00'), '%') AS value_
            FROM metric_pivot
        
            UNION ALL
          
            SELECT 5 AS _order_
                 , '% Change vs Prior Year' AS metric
                 , concat(to_varchar(perc_change_vs_prior_year, 'S999,999,999,999.00'), '%') AS value_
            FROM metric_pivot
        )
        
        
        SELECT metric AS "Metric"
             , value_ AS "Value"
        FROM unpivot
        ORDER BY _order_
        LIMIT 10000
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


def get_platform_fees_stats(
        TimePeriodDays: int = 365,
        Marketplace: str = "All",
        ContractAddress: str = "",
        WashTradeFilter: str = "On",
        Currency: str = "USD") -> pd.DataFrame:
    """

    :return:
    """
    query = f"""
    WITH
        nft_sales AS (
            SELECT s.tx_hash
                 , s.block_timestamp
                 , s.buyer_address
                 , s.seller_address
                 , s.platform_name
                 , s.nft_address
                 , s.tokenid AS token_id
                 , s.price_usd
                 , s.creator_fee_usd
                 , s.platform_fee_usd
                 , (CASE WHEN s.currency_symbol IN ('ETH','WETH') THEN s.price
                         ELSE s.price_usd / p.price END) AS price_eth
                 , (CASE WHEN s.currency_symbol IN ('ETH','WETH') THEN s.creator_fee
                         ELSE s.creator_fee_usd / p.price END) AS creator_fee_eth
                 , (CASE WHEN s.currency_symbol IN ('ETH','WETH') THEN s.platform_fee
                         ELSE s.platform_fee_usd / p.price END) AS platform_fee_eth
              
            FROM ethereum.core.ez_nft_sales AS s
                LEFT JOIN ethereum.core.fact_hourly_token_prices AS p
                    ON p.hour = date_trunc('hour', s.block_timestamp)
                    AND p.symbol = 'WETH'
        
            WHERE True
          -- Exclude non-collectibles
              AND nft_address NOT IN ( '0xc36442b4a4522e871399cd717abdd847ab11fe88' -- Uniswap V3
                                     , '0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85' -- ENS
                                     , '0x58a3c68e2d3aaf316239c003779f71acb870ee47' -- Curve Finance
                                     )
           -- Filter out weird data points
              AND NOT (creator_fee < 0)
              AND s.price > 0
              AND NOT (currency_symbol = 'ETH' AND currency_address NOT IN ('ETH', '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'))  -- incorrect currency
          
          -- Dashboard filters
              AND block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
              AND (CASE WHEN lower('{Marketplace}') IN ('', '-', 'all', 'combined') THEN True
                        ELSE lower('{Marketplace}') = platform_name END)
              AND (CASE WHEN '{ContractAddress}' = '' THEN True
                        WHEN '{ContractAddress}' = '-' THEN True
                        WHEN '{ContractAddress}' = 'None' THEN True
                        WHEN '{ContractAddress}' = 'All' THEN True
                        WHEN '{ContractAddress}' IS NULL THEN True
                        ELSE lower('{ContractAddress}') = nft_address END)
        ),
        
        
        wash_trades AS (
         -- Query logic forked from:
         --   https://app.flipsidecrypto.com/velocity/queries/19bb605d-ef5b-4d76-b84e-386c93341f12
         --   by pinehearst
          
            WITH 
                
            eth_tx AS ( -- group day, A, B transfers 
                SELECT utc_date
                     , eth_from_address -- A 
                     , eth_to_address   -- B 
                     , sum(eth) as eth_transferred -- ETH/WETH transferred for the day 
                
                FROM (
                    SELECT tx_hash
                         , date_trunc('day', block_timestamp) AS utc_date
                         , eth_from_address -- A
                         , eth_to_address   -- B 
                         , amount AS eth 
                    FROM ethereum.core.ez_eth_transfers
                    WHERE block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
                      AND eth_from_address IN (SELECT DISTINCT buyer_address FROM ethereum.core.ez_nft_sales) -- filter out users that traded NFT 
                      AND tx_hash NOT IN (SELECT DISTINCT tx_hash FROM ethereum.core.ez_nft_sales) -- filter out ETH transfer from buying/selling 
                    
                    UNION 
                    
                    SELECT tx_hash
                         , date_trunc('day', block_timestamp) AS utc_date
                         , from_address AS eth_from_address -- A 
                         , to_address  AS eth_to_address    -- B 
                         , amount AS eth
                    FROM ethereum.core.ez_token_transfers
                    WHERE contract_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                      AND block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
                      AND from_address IN (SELECT DISTINCT buyer_address FROM ethereum.core.ez_nft_sales) -- filter out users that traded NFT 
                      AND tx_hash NOT IN (SELECT DISTINCT tx_hash FROM ethereum.core.ez_nft_sales) -- filter out WETH transfer from buying/selling i.e. back end WETH transfers 
                ) AS tx
                
                GROUP BY 1,2,3 
            ),
            
              
            pos_wash_tx AS (
                SELECT DISTINCT tx_hash
                FROM (
                    SELECT a.block_timestamp
                         , a.platform_name
                         , a.tx_hash
                         , datediff('minute', a.block_timestamp, b.block_timestamp) AS minutes_diff
                         , a.buyer_address AS buyerB -- B 
                         , b.seller_address AS sellerB -- B 
                         , a.seller_address AS sellerA -- A 
                         , b.buyer_address AS buyerA -- A 
                
                    FROM ethereum.core.ez_nft_sales AS a
                        LEFT JOIN ethereum.core.ez_nft_sales AS b 
                            ON a.nft_address = b.nft_address 
                            AND a.tokenid = b.tokenid 
                            AND a.buyer_address = b.seller_address -- buyer was a seller of the nft  	
                            AND a.seller_address = b.buyer_address -- seller was a buyer of the nft 
              
                    WHERE a.block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
                ) AS tx
                WHERE sellerB IS NOT NULL AND buyerA IS NOT NULL 
            ),
            
              
            labelled_trades AS (
                SELECT s.tx_hash
                     , s.block_timestamp
                     , s.platform_name
                     , s.buyer_address  -- B 
                     , s.seller_address -- A 
                     , etx.eth_transferred -- will be NULL if buyer and seller has no ETH/WETH transfer for the day 
                     , pw.tx_hash AS pos_wash_tx -- will be NULL if it wasn't a wash trading A->B->A 
                     , s.price
                     , s.price_usd
                     , s.nft_address
                     , s.project_name
                     , s.tokenid
              
                     , (CASE WHEN etx.eth_transferred IS NOT NULL AND pw.tx_hash IS NOT NULL THEN 'PoS & Self-Fund Wash' -- highly probably 
                             WHEN etx.eth_transferred IS NULL     AND pw.tx_hash IS NOT NULL THEN 'PoS Wash'             -- very likely wash  
                             WHEN etx.eth_transferred IS NOT NULL AND pw.tx_hash IS NULL     THEN 'Self-Fund Wash'       -- likely wash  
                             WHEN etx.eth_transferred IS NULL     AND pw.tx_hash IS NULL     THEN 'Organic'              -- organic   
                             END) AS sales_type
              
                FROM ethereum.core.ez_nft_sales AS s
                    LEFT JOIN eth_tx AS etx
                        ON s.buyer_address = etx.eth_to_address -- buyer (B) received ETH from...
                        AND s.seller_address = etx.eth_from_address -- seller (A)  
                        AND s.block_timestamp::date = etx.utc_date -- on the same day on the same day of sales 
                  
                    LEFT JOIN pos_wash_tx AS pw
                        ON pw.tx_hash = s.tx_hash
                
                WHERE s.block_timestamp >= CURRENT_DATE - interval '1 years' - interval '{TimePeriodDays} days' - interval '1 month'
                    AND s.currency_symbol IN ('WETH', 'ETH') -- Filter sales in WETH/ETH
            )
            
            
            SELECT DISTINCT tx_hash, nft_address, tokenid
            FROM labelled_trades
            WHERE sales_type != 'Organic'
        ),
        
        
        nft_sales__wash_trade_filter_off AS (
            SELECT * 
            FROM nft_sales
        ),
        
        
        nft_sales__wash_trade_filter_on AS (
            SELECT * 
            FROM nft_sales
            WHERE tx_hash NOT IN (SELECT tx_hash FROM wash_trades)
        ),
        
        
        daily_trader_stats AS (
            SELECT utc_date
                 , trader
        
                 , (CASE WHEN utc_date BETWEEN CURRENT_DATE - interval '{TimePeriodDays} days'
                                           AND CURRENT_DATE - interval '1 day' 
                                          THEN 'Current Period'
          
                         WHEN utc_date BETWEEN CURRENT_DATE - interval '{TimePeriodDays} days' - interval '{TimePeriodDays} days' 
                                           AND CURRENT_DATE - interval '{TimePeriodDays} days' - interval '1 day'
                                          THEN 'Prior Period'
          
                         WHEN utc_date BETWEEN CURRENT_DATE - interval '1 year' - interval '{TimePeriodDays} days'
                                           AND CURRENT_DATE - interval '1 year' - interval '1 day'
                                          THEN 'Prior Year'
          
                         ELSE '-' END) AS period
          
                 , sum(CASE WHEN tx_type = 'buy' THEN tx_count ELSE 0 END) AS buy_txs
                 , sum(CASE WHEN tx_type = 'buy' THEN volume_usd ELSE 0 END) AS buy_volume_usd
                 , sum(CASE WHEN tx_type = 'buy' THEN volume_eth ELSE 0 END) AS buy_volume_eth
                 , sum(CASE WHEN tx_type = 'sell' THEN tx_count ELSE 0 END) AS sell_txs
                 , sum(CASE WHEN tx_type = 'sell' THEN volume_usd ELSE 0 END) AS sell_volume_usd
                 , sum(CASE WHEN tx_type = 'sell' THEN volume_eth ELSE 0 END) AS sell_volume_eth
                 , sum(tx_count) AS total_txs
                 , sum(volume_usd) AS total_volume_usd
                 , sum(volume_eth) AS total_volume_eth
                 , sum(creator_fees_usd) AS total_creator_fees_usd
                 , sum(creator_fees_eth) AS total_creator_fees_eth
                 , sum(platform_fees_usd) AS total_platform_fees_usd
                 , sum(platform_fees_eth) AS total_platform_fees_eth
          
            FROM (
                SELECT block_timestamp::date AS utc_date
                     , buyer_address AS trader
                     , 'buy' AS tx_type
                     , count(*) AS tx_count
                     , sum(price_usd) AS volume_usd
                     , sum(price_eth) AS volume_eth
                     , sum(0) AS creator_fees_usd
                     , sum(0) AS creator_fees_eth
                     , sum(0) AS platform_fees_usd
                     , sum(0) AS platform_fees_eth
                FROM nft_sales__wash_trade_filter_{WashTradeFilter}
                GROUP BY 1,2,3
        
                UNION ALL
          
                SELECT block_timestamp::date AS utc_date
                     , seller_address AS trader
                     , 'sell' AS tx_type
                     , count(*) AS tx_count
                     , sum(price_usd) AS volume_usd
                     , sum(price_eth) AS volume_eth
                     , sum(creator_fee_usd) AS creator_fees_usd
                     , sum(creator_fee_eth) AS creator_fees_eth
                     , sum(platform_fee_usd) AS platform_fees_usd
                     , sum(platform_fee_eth) AS platform_fees_eth
                FROM nft_sales__wash_trade_filter_{WashTradeFilter}
                GROUP BY 1,2,3
            ) AS t
        
            GROUP BY 1,2,3
        ),
        
        
        trader_summary_stats AS (
            SELECT trader
                 , period
                 , sum(total_txs) AS count_trades
                 , sum(total_volume_usd) AS volume_usd
                 , sum(total_volume_eth) AS volume_eth
                 , sum(total_creator_fees_usd) AS creator_fees_usd
                 , sum(total_creator_fees_eth) AS creator_fees_eth
                 , sum(total_platform_fees_usd) AS platform_fees_usd
                 , sum(total_platform_fees_eth) AS platform_fees_eth
            FROM daily_trader_stats
            WHERE period IN ('Current Period', 'Prior Period', 'Prior Year')
            GROUP BY 1,2
        ),
        
        
        period_summary_stats AS (
            SELECT period
                 , count(trader) AS wallets
                 , count(CASE WHEN count_trades = 1 THEN trader ELSE NULL END) AS wallets__single_tx
                 , count(CASE WHEN count_trades > 1 THEN trader ELSE NULL END) AS wallets__multiple_tx
          
                 , sum(volume_usd) / 2 AS trading_volume__usd
                 , sum(volume_eth) / 2 AS trading_volume__eth
          
                 , sum(volume_usd) / count(trader) AS volume_per_wallet__usd
                 , sum(volume_eth) / count(trader) AS volume_per_wallet__eth
                 , sum(count_trades) / count(trader) AS transactions_per_wallet
                 , sum(volume_usd) / sum(count_trades) AS volume_per_transaction__usd
                 , sum(volume_eth) / sum(count_trades) AS volume_per_transaction__eth
        
                 , sum(creator_fees_usd) AS creator_fees__usd
                 , sum(creator_fees_eth) AS creator_fees__eth
                 , sum(platform_fees_usd) AS platform_fees__usd
                 , sum(platform_fees_eth) AS platform_fees__eth
        
                 , trading_volume__{Currency} AS trading_volume
                 , volume_per_wallet__{Currency} AS volume_per_wallet
                 , volume_per_transaction__{Currency} AS volume_per_transaction
                 , creator_fees__{Currency} AS creator_fees
                 , platform_fees__{Currency} AS platform_fees
          
            FROM trader_summary_stats
            WHERE count_trades >= 1 
            GROUP BY 1
        ),
        
        
        metric_pivot AS (
            SELECT 'Creator Fees' AS metric
                 , (CASE WHEN '{Currency}' = 'ETH' THEN 'Ξ'
                         WHEN '{Currency}' = 'USD' THEN '$' END) AS currency_symbol
                 , avg(CASE WHEN period = 'Current Period' THEN platform_fees ELSE NULL END) AS value__current_period
                 , avg(CASE WHEN period = 'Prior Period' THEN platform_fees ELSE NULL END) AS value__prior_period
                 , avg(CASE WHEN period = 'Prior Year' THEN platform_fees ELSE NULL END) AS value__prior_year
                 , 100.00 * (value__current_period / nullif(value__prior_period, 0) - 1) AS perc_change_vs_prior_period
                 , 100.00 * (value__current_period / nullif(value__prior_year, 0) - 1) AS perc_change_vs_prior_year
            FROM period_summary_stats
            GROUP BY 1,2
        ),
        
        
        unpivot AS (
            SELECT 1 AS _order_
                 , 'Current Period' AS metric
                 , concat(currency_symbol, ' ', to_varchar(value__current_period, '999,999,999,999.0000')) AS value_
            FROM metric_pivot
        
            UNION ALL
          
            SELECT 2 AS _order_
                 , 'Prior Period' AS metric
                 , concat(currency_symbol, ' ', to_varchar(value__prior_period, '999,999,999,999.0000')) AS value_
            FROM metric_pivot
        
            UNION ALL
          
            SELECT 3 AS _order_
                 , 'Prior Year' AS metric
                 , concat(currency_symbol, ' ', to_varchar(value__prior_year, '999,999,999,999.0000')) AS value_
            FROM metric_pivot
        
            UNION ALL
          
            SELECT 4 AS _order_
                 , '% Change vs Prior Period' AS metric
                 , concat(to_varchar(perc_change_vs_prior_period, 'S999,999,999,999.00'), '%') AS value_
            FROM metric_pivot
        
            UNION ALL
          
            SELECT 5 AS _order_
                 , '% Change vs Prior Year' AS metric
                 , concat(to_varchar(perc_change_vs_prior_year, 'S999,999,999,999.00'), '%') AS value_
            FROM metric_pivot
        )
        
        
        SELECT metric AS "Metric"
             , value_ AS "Value"
        FROM unpivot
        ORDER BY _order_
        LIMIT 10000
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
