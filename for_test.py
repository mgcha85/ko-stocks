import marimo

__generated_with = "0.10.9"
app = marimo.App(width="medium")


@app.cell(disabled=True)
def _():
    import yfinance as yf
    import pandas as pd
    import requests

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


    def download_krx_stock_list():
        """
        KRX 상장법인목록 엑셀 파일을 POST 요청으로 다운로드하고, pandas로 읽어들여 반환합니다.

        Returns:
            pd.DataFrame: 상장 종목 데이터프레임
        """
        # POST 요청 URL
        url = "https://kind.krx.co.kr/corpgeneral/corpList.do"

        # POST 요청에 필요한 데이터 (폼 데이터)
        payload = {
            "method": "download",
            "pageIndex": "1",
            "currentPageSize": "5000"  # 최대 5000개 종목을 가져오도록 설정
        }

        # 헤더 정보
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": "https://kind.krx.co.kr/corpgeneral/corpList.do?method=loadInitPage"
        }

        # POST 요청 보내기
        response = requests.post(url, data=payload, headers=headers)
        response.raise_for_status()  # 요청 실패 시 예외 발생

        # 응답 내용을 엑셀 파일로 저장
        file_name = "krx_stock_list.xls"
        with open(file_name, "wb") as file:
            file.write(response.content)
    return download_krx_stock_list, get_stock_data, pd, requests, yf


@app.cell(disabled=True)
def _(download_krx_stock_list):
    # 함수 실행
    krx_stocks = download_krx_stock_list()
    return (krx_stocks,)


@app.cell(disabled=True)
def _(pd):
    df = pd.concat([
        pd.read_excel("코스피상장법인목록.xlsx", dtype=str).assign(type='KS'),
        pd.read_excel("코스닥상장법인목록.xlsx", dtype=str).assign(type='KQ')
    ])
    if "종목코드" in df.columns:
        df["종목코드"] = df["종목코드"].str.zfill(6)
    df
    return (df,)


@app.cell(disabled=True)
def _(df, get_stock_data):
    from datetime import datetime
    from dateutil.relativedelta import relativedelta
    import sqlite3


    con = sqlite3.connect('kr_stocklist.sqlite3')
    # 오늘 날짜 (end_date)
    end_date = datetime.today().strftime("%Y-%m-%d")

    # 10년 전 날짜 (start_date)
    start_date = (datetime.today() - relativedelta(years=10)).strftime("%Y-%m-%d")

    # offset = 1618
    for i, (ticker, type) in enumerate(zip(df["종목코드"], df["type"])):
        # if i < offset:
        #     continue

        # 삼성전자 티커 심볼
        ticker_symbol = f"{ticker}.{type}"  # 한국 거래소(KRX)에서 티커

        stock_data = get_stock_data(ticker_symbol, start_date, end_date)
        if stock_data.empty:
            continue
        stock_data.columns = ['Close', 'High', 'Low', 'Open', 'Volume']
        stock_data.to_sql(ticker_symbol, con, if_exists='replace')
    return (
        con,
        datetime,
        end_date,
        i,
        relativedelta,
        sqlite3,
        start_date,
        stock_data,
        ticker,
        ticker_symbol,
        type,
    )


@app.cell(disabled=True)
def _(end_date, get_stock_data, start_date):
    ticker_symbol1 = "005930.KS"
    stock_data1 = get_stock_data(ticker_symbol1, start_date, end_date)
    print(stock_data1)
    return stock_data1, ticker_symbol1


@app.cell(disabled=True)
def _():
    return


@app.cell(disabled=True)
def _(date_str):
    from pykrx import stock
    import sqlite3
    import pandas as pd
    from datetime import datetime
    from dateutil.relativedelta import relativedelta
    from tqdm import tqdm

    # 오늘 날짜 (end_date)
    end_date = datetime.today()

    # 10년 전 날짜 (start_date)
    start_date = datetime.today() - relativedelta(years=10)

    # 삼성전자('005930')의 일별 시가총액 조회
    con_fund = sqlite3.connect('funda.sqlite3')

    # 날짜 범위 생성
    date_range = pd.date_range(start=start_date, end=end_date, freq='B')  # 'B'는 영업일(주말 제외)을 의미

    # 데이터 수집 및 저장
    for date in tqdm(date_range, desc="Collecting fundamental data"):
        table_name = date.strftime('%Y%m%d')
        try:
            # 해당 날짜의 펀더멘털 데이터 조회
            fundamental_df = stock.get_market_fundamental_by_ticker(table_name)
            
            # 데이터베이스에 저장
            fundamental_df.to_sql(table_name, con_fund, if_exists='replace')
        except Exception as e:
            print(f"Error on {date_str}: {e}")

    # 데이터베이스 연결 종료
    con_fund.close()
    return (
        con_fund,
        date,
        date_range,
        datetime,
        end_date,
        fundamental_df,
        pd,
        relativedelta,
        sqlite3,
        start_date,
        stock,
        table_name,
        tqdm,
    )


@app.cell(disabled=True)
def _():
    return


if __name__ == "__main__":
    app.run()
