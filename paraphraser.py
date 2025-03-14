import pandas as pd


def paraphrase(df):
    df["query"] = df["query_base"]
    df["expertise"] = -1
    df["formality"] = -1
    return df
