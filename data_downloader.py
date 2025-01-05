from pykrx import stock
import yfinance as yf
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import sqlite3
from common.utils import getAllStockCode


def table_exists(con, table_name):
    """
    데이터베이스에 특정 테이블이 존재하는지 확인합니다.

    Parameters:
        con (sqlite3.Connection): SQLite 연결 객체
        table_name (str): 확인할 테이블 이름

    Returns:
        bool: 테이블 존재 여부
    """
    query = "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?;"
    cursor = con.execute(query, (table_name,))
    return cursor.fetchone()[0] == 1

def get_latest_date(con, table_name):
    """
    테이블에서 Date 컬럼의 가장 최신 날짜를 가져옵니다.

    Parameters:
        con (sqlite3.Connection): SQLite 연결 객체
        table_name (str): 대상 테이블 이름

    Returns:
        str or None: 최신 날짜 (YYYY-MM-DD 형식), 데이터가 없으면 None
    """
    query = f"SELECT MAX(Date) FROM {table_name};"
    cursor = con.execute(query)
    result = cursor.fetchone()[0]
    if result:
        return pd.to_datetime(result).strftime('%Y-%m-%d')
    return None

# 데이터 가져오기
def get_stock_data(ticker, start_date="2020-01-01", end_date=None):
    """
    Yahoo Finance에서 주식 데이터를 가져옵니다.

    Parameters:
        ticker (str): 주식 티커
        start_date (str): 시작 날짜 (YYYY-MM-DD 형식)
        end_date (str): 종료 날짜 (YYYY-MM-DD 형식), None일 경우 오늘 날짜

    Returns:
        pd.DataFrame: 일봉 데이터
    """
    stock_data = yf.download(ticker, start=start_date, end=end_date, interval="1d")
    return stock_data


def download():
    df = getAllStockCode()
    con = sqlite3.connect('kr_stocklist.sqlite3')

    # offset = 1618
    for _, (ticker, type) in enumerate(zip(df["종목코드"], df["type"])):
        # if i < offset:
        #     continue
        
        # 티커 심볼
        ticker_symbol = f"{ticker}.{type}"  # 한국 거래소(KRX)에서 티커
        end_date = datetime.today().strftime('%Y-%m-%d')

        if table_exists(con, ticker_symbol):
            latest_date = get_latest_date(con, ticker_symbol)
            if latest_date:
                start_date = (pd.to_datetime(latest_date) + relativedelta(days=1)).strftime('%Y-%m-%d')
            else:
                start_date = (datetime.today() - relativedelta(years=10)).strftime('%Y-%m-%d')
        else:
            start_date = (datetime.today() - relativedelta(years=10)).strftime('%Y-%m-%d')

        stock_data = get_stock_data(ticker_symbol, start_date, end_date)
        if stock_data.empty:
            continue

        if start_date <= end_date:
            # market cap
            market_cap = stock.get_market_cap_by_date(start_date, end_date, ticker)
            if market_cap.empty:
                continue
            market_cap.index.name = 'Date'

            stock_data.columns = ['Close', 'High', 'Low', 'Open', 'Volume']

            stock_data = stock_data.join(market_cap, how='left')
            stock_data.to_sql(ticker_symbol, con, if_exists='replace')
