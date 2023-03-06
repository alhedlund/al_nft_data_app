import pandas as pd
from loguru import logger


def flatten(d):
    """

    :param d:
    :return:
    """
    out = {}
    for key, val in d.items():
        if isinstance(val, dict):
            val = [val]
        if isinstance(val, list):
            for subdict in val:
                deeper = flatten(subdict).items()
                out.update({key + '_' + key2: val2 for key2, val2 in deeper})
        else:
            out[key] = val
    return out


def add_pct_change_cols(df: pd.DataFrame, categorical_col: str) -> pd.DataFrame:
    """
    Takes a dataframe with all numerical cols and gets the
    period-over-period pct change.

    :param df: dataframe with all numerical cols
    :param categorical_col: name of categorical col
    :return transformed_df: dataframe with p/p cols added
    """
    transformed_df_list = []

    for chain in list(df[categorical_col].unique()):
        single_chain_df = df[df[categorical_col] == chain]

        single_chain_df["date"] = pd.to_datetime(single_chain_df["date"])

        single_chain_df.set_index(keys=["date", categorical_col], drop=True, inplace=True)

        for col in single_chain_df.columns:
            single_chain_df[col + " W/W"] = single_chain_df[col].pct_change(1)

        transformed_df_list.append(single_chain_df)

    transformed_df = pd.concat(transformed_df_list)

    transformed_df.reset_index(inplace=True)

    return transformed_df


def add_avg_trade_size_by_mktplace_cols(df: pd.DataFrame) -> pd.DataFrame:
    try:
        df['avg. trade size by marketplace'] = df['volume_usd'] / df['trades']

        return df

    except Exception as e:
        logger.info(f"Function call flagged the following exception: {e}")


def calc_cat_share_by_metric(df: pd.DataFrame, cat_col: str, metric_col: str) -> pd.DataFrame:
    try:
        piv_df = (df
                  .pivot(index=['date'], columns=[cat_col], values=[metric_col])
                  .reset_index()
                  )

        pct_piv_df = piv_df.set_index('date').apply(lambda x: (x / x.sum()) * 100, axis=1)

        return pct_piv_df

    except Exception as e:
        logger.info(f"Function call flagged the following exception: {e}")
