import pandas as pd
import numpy as np

def _roll_prev_value(df, current_bar, prev_bar):
    # might wanna return, just to be sure?
    df.loc[current_bar] = df.iloc[prev_bar]
    
def _roll_prev_value_np(df, current_bar, prev_bar):
    # might wanna return, just to be sure?
    df[current_bar] = df[prev_bar]

def _remove_dups(data):
    data = data.ffill()
    data = data.where(data != data.shift(1))
    return data

def _find_signals(df):
    # replacing df[0] with False
    # return df.
    return df.where(df != df.shift(1).fillna(df.iloc[0]))

def _find_affected_assets(df, current_bar):
    return df.loc[current_bar].notna().values

def _aggregate(agg_df, df, ax=1):
    return pd.concat([agg_df, df], axis=ax)  

def _prep_and_agg_custom_stops(agg_df, df, name, ax=1):
    df.name = name
    # replace pos and neg inf that might result if C==L, in which case we dont want to allocate anything
    df.replace(np.Inf, 0, inplace=True)
    df.replace(-np.Inf, 0, inplace=True)
    
    df2 = _aggregate(agg_df, df, ax)
    return df2

def _find_df(df, name):
    for i in range(len(df)):
        if name == df[i][0]:
            return df[i][1]
