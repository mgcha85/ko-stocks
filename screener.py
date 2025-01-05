import sqlite3
import pandas as pd


def get_all_tables(conn):
    """
    SQLite 데이터베이스의 모든 테이블 목록을 가져와 리스트로 반환합니다.
    
    Parameters:
        database_path (str): SQLite 데이터베이스 파일 경로
    
    Returns:
        list: 테이블 이름 리스트
    """
    try:
        cursor = conn.cursor()

        # sqlite_master에서 테이블 이름 가져오기
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        # 테이블 이름 리스트로 변환
        table_list = [table[0] for table in tables]

        return table_list

    except sqlite3.Error as e:
        print(f"SQLite 오류가 발생했습니다: {e}")
        return []


def set_signal(df):
    """
    volume 증가 및 장대양봉 신호를 감지합니다.
    
    Parameters:
        df (pd.DataFrame): 종목 데이터 (columns: 'Date', 'Close', 'Open', 'High', 'Low', 'Volume')
    
    Returns:
        pd.DataFrame: 신호가 감지된 데이터
    """
    # 장대양봉 조건: (high - low) > 특정 기준 & 종가 > 시가
    df['COR'] = (df['Close'] - df['Open']) / df['Open']
    df['vrate'] = df['Volume'] / df['Volume'].rolling(window=60).mean()
    return df


def identify_candle_signal(df_cor, df_vrate):
    tickers1 = set(df_vrate[df_vrate > 8].index)
    if len(tickers1) == 0:
        return
    
    df_cor = df_cor.loc[tickers1]
    tickers2 = set(df_cor[df_cor > 0.03].index)
    if len(tickers2) == 0:
        return
    
    return list(set.intersection(tickers1, tickers2))[:30]


def set_moving_average(df):
    for interval in [20, 60, 200]:
        ma = df['Close'].rolling(interval).mean()
        df[f'ma{interval}pct'] = (df['Close'] - ma) / ma
    return df


if __name__ == '__main__':
    # SQLite 데이터베이스 연결
    database_path = "kr_stocklist.sqlite3"
    conn = sqlite3.connect(database_path)

    table_list = get_all_tables(conn)
    dfs = []
    dates = []
    for ticker in table_list:
        df = pd.read_sql(f"SELECT * FROM '{ticker}' WHERE Date>'2019-07-01'", conn)
        if df.shape[0] < 1000:
            continue

        df = df.assign(ticker=ticker).assign(market=ticker.split('.')[1])
        df = set_signal(df)
        df = set_moving_average(df)
        dfs.append(df)
        # dates.append(df.index)
    
    df = pd.concat(dfs)
    conn_scr = sqlite3.connect('screener.sqlite3')

    for market, df_market in df.groupby('market'):
        print(market)
        cor_screener = df_market.pivot_table(index="Date", columns="ticker", values='COR')
        vrate_screener = df_market.pivot_table(index="Date", columns="ticker", values='vrate')
        mapct_screener = df_market.pivot_table(index="Date", columns="ticker", values='ma200pct')

        cor_screener.to_sql(f'cor.{market}', conn_scr, if_exists='replace')
        vrate_screener.to_sql(f'vrate.{market}', conn_scr, if_exists='replace')
        mapct_screener.to_sql(f'mapct.{market}', conn_scr, if_exists='replace')

    conn.close()
    conn_scr.close()
