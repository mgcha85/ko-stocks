from pykrx import stock
import sqlite3
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from tqdm import tqdm
from common.utils import getStockCode


def get_trade_amount(start_date, end_date):
    con = sqlite3.connect('trade_amount.sqlite3')

    df = pd.concat(
        [getStockCode(market).assign(type=code) 
         for market, code in zip(['kosdaq', 'kospi'], ['KQ', 'KS'])]
    )

    # offset = 477
    for i, (ticker, type) in tqdm(enumerate(zip(df["종목코드"], df["type"]))):
        # if i < offset:
        #     continue
        # 티커 심볼
        ticker_symbol = f"{ticker}.{type}"  # 한국 거래소(KRX)에서 티커
        try:
            market_cap = stock.get_market_cap_by_date(start_date, end_date, ticker)
            if market_cap.empty:
                continue
            market_cap.index.name = 'Date'
            market_cap.to_sql(ticker_symbol, con, if_exists='replace')
        except Exception as e:
            print(f"Error on {ticker_symbol}: {e}")

    # 데이터베이스 연결 종료
    con.close()

def get_fundamental(start_date, end_date):
    # 삼성전자('005930')의 일별 시가총액 조회
    con = sqlite3.connect('fundamental.sqlite3')

    # 날짜 범위 생성
    date_range = pd.date_range(start=start_date, end=end_date, freq='B')  # 'B'는 영업일(주말 제외)을 의미

    # 데이터 수집 및 저장
    for date in tqdm(date_range, desc="Collecting fundamental data"):
        table_name = date.strftime('%Y%m%d')
        try:
            # 해당 날짜의 펀더멘털 데이터 조회
            fundamental_df = stock.get_market_fundamental_by_ticker(table_name)
            
            # 데이터베이스에 저장
            fundamental_df.to_sql(table_name, con, if_exists='replace')
        except Exception as e:
            print(f"Error on {table_name}: {e}")

    # 데이터베이스 연결 종료
    con.close()


if __name__ == '__main__':
    # 오늘 날짜 (end_date)
    end_date = (datetime.today() - relativedelta(days=1)).strftime("%Y-%m-%d") 

    # 10년 전 날짜 (start_date)
    start_date = (datetime.today() - relativedelta(years=10, days=1)).strftime("%Y-%m-%d")
    get_trade_amount(start_date, end_date)
    