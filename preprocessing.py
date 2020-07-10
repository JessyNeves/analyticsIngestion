import pandas as pd

def preprocess(pandas_dataframe):
    """Cast date to datetype"""
    pandas_dataframe["date"] = pd.to_datetime(pandas_dataframe["date"])

    """Drop Nas"""
    pandas_dataframe.dropna(subset=["date", "visitid", "fullvisitorid", "Sessions", "action", "action_desc"], inplace=True)
    return pandas_dataframe
