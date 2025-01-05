import pandas as pd
import numpy as np
import sqlite3


con = sqlite3.connect('kr_stocklist.sqlite3')

## Step 1: load results
df_result = pd.concat(
    [pd.read_excel('results/KQ.results.xlsx'), pd.read_excel('results/KS.results.xlsx')]
).sort_values(by='buy_date')

dfs = {ticker: pd.read_sql(f"SELECT * FROM '{ticker}'", con) for ticker in df_result['ticker'].unique()}

for idx, row in df_result.iterrows():
    buy_date = row['buy_date']
    ticker = row['ticker']

    df = dfs[ticker]
    df_buy = df[df['Date'] <= buy_date]