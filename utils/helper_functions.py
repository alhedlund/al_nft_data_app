import pandas as pd

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


def add_pct_change_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes a dataframe with all numerical cols and gets the
    period-over-period pct change.

    :param df: dataframe with all numerical cols
    :return transformed_df: dataframe with p/p cols added
    """
    transformed_df_list = []

    for chain in list(df["blockchain"].unique()):
        single_chain_df = df[df["blockchain"] == chain]

        single_chain_df["date"] = pd.to_datetime(single_chain_df["date"])

        single_chain_df.set_index(keys=["date", "blockchain"], drop=True, inplace=True)

        for col in single_chain_df.columns:
            single_chain_df[col + " W/W"] = single_chain_df[col].pct_change(1)

        transformed_df_list.append(single_chain_df)

    transformed_df = pd.concat(transformed_df_list)

    transformed_df.reset_index(inplace=True)

    return transformed_df
