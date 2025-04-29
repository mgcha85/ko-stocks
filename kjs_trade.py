import sqlite3
import pandas as pd
from datetime import datetime
from pykrx import stock
import os


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

# 매수 전략 계산

def calculate_buy_points(first_price):
    """
    매수 포인트와 각 단계에서 구매할 수량 계산

    Parameters:
        seed (float): 초기 투자 금액
        first_price (float): 첫 번째 매수 가격

    Returns:
        list: 매수 포인트와 매수 금액
    """
    positions = []
    positions.append(first_price)

    for i in range(1, 4):
        next_price = positions[-1] * 0.9  # 이전 가격의 -10%
        positions.append(next_price)

    return positions

# 매도 전략 계산

def calculate_sell_point(buy_price):
    """
    매도 가격 계산 (평균 매수 단가의 +10%)

    Parameters:
        average_price (float): 평균 매수 가격

    Returns:
        float: 매도 목표 가격
    """
    return buy_price * 1.1

# 백테스트 수행

def run_backtest(root, screener_data, dfs, n_split=4):
    """
    백테스트 실행

    Parameters:
        screener_data (pd.DataFrame): 선정된 종목 데이터 (Date, ticker)
        price_data (pd.DataFrame): 종목 가격 데이터
        seed (float): 초기 투자 금액

    Returns:
        pd.DataFrame: 백테스트 결과
    """
    database_path = os.path.join(root, "fundamental.sqlite3")
    conn_fund = sqlite3.connect(database_path)

    print(screener_data.head())

    results = []
    hold_list = []
    for date, each in screener_data.groupby('Date'):
        if date >= '2025-04-22':
            continue
        df_fund = pd.read_sql(f"SELECT * FROM '{convert_datetime_string(date)}'", conn_fund, index_col='티커')

        for _, row in each.iterrows():
            ticker = row['ticker']
            cor = row['cor']
            vrate = row['vrate']
            mapct = row['ma200pct']

            if ticker in hold_list:
                continue

            if ticker not in dfs:
                continue

            if len(hold_list) < 200:
                prices = dfs[ticker]
                next_prices = prices[prices.index > date]

                # 매수 포인트 계산
                buy_price = prices.loc[date, 'Close']
                pv = prices.loc[date, '거래대금']
                amount = prices.loc[date, '시가총액']
                buy_points = calculate_buy_points(buy_price)

                hold_list.append(ticker)
                sell_price = calculate_sell_point(buy_price)
                symbol = ticker.split('.')[0]

                if symbol not in df_fund.index:
                    fundamental = pd.Series(index=df_fund.columns)
                else:
                    fundamental = df_fund.loc[ticker.split('.')[0]]

                days_max_high = days_since_max_high(prices, date.split()[0], window_days=600)
                krx_date = convert_datetime_string(date)
                # kospi_close = fetch_index_close(krx_date, market='KOSPI')

                order = 1
                results.append({
                    'ticker': ticker,
                    'buy_date': date,
                    'buy_price': buy_price,
                    'sell_date': None,
                    'sell_price': None,
                    'profit_pct': None,
                    'cor': cor,
                    'vrate': vrate,
                    'mapct': mapct,
                    'order': order,
                    '거래대금': pv,
                    '시가총액': amount,
                    'duration': None,
                    'days_since_max_high': days_max_high,
                    # 'kospi_index': kospi_close,
                    **fundamental.to_dict()
                })

                for sell_date, each in next_prices.iterrows():
                    duration = (pd.to_datetime(sell_date) - pd.to_datetime(date)).days

                    # ─── 1) 보유 30일 초과 & 당일 10% 이상 상승 시 즉시 매도 ───
                    # 직전 종가(prev_close) 대비 당일 고가(High)로 계산하거나,
                    # 당일 종가(Close) 상승폭을 봐도 됩니다. 예시는 prev_close 기준.
                    # if duration > 30:
                    #     # 첫 루프의 prev_close는 매수가(buy_price)로 초기화
                    #     if 'prev_close' in locals():
                    #         prev = prev_close
                    #     else:
                    #         prev = buy_price
                    
                    #     # 당일 고가 기준 일일 상승률
                    #     intraday_pct = (each['High'] - prev) / prev
                    #     if intraday_pct >= 0.1:
                    #         sell_price = each['Close']    # 또는 each['High']로 지정
                    #         profit_pct = (sell_price - buy_price) / buy_price
                    #         results[-1].update({
                    #             'sell_date':  sell_date,
                    #             'sell_price': sell_price,
                    #             'profit_pct': profit_pct,
                    #             'duration':   duration
                    #         })
                    #         hold_list.remove(ticker)
                    #         break
                    
                    # 매 루프 끝에 prev_close 갱신
                    # prev_close = each['Close']

                    if order < n_split and each['Low'] < buy_points[order]:
                        order += 1
                        buy_price = sum(buy_points[:order]) / order
                        sell_price = calculate_sell_point(buy_price)
                        results[-1]['buy_price'] = buy_price
                        results[-1]['order'] = order

                    elif each['High'] > sell_price:
                        profit_pct = (sell_price - buy_price) / buy_price
                        duration = (pd.to_datetime(sell_date) - pd.to_datetime(date)).days
                        hold_list.remove(ticker)

                        results[-1]['sell_date'] = sell_date
                        results[-1]['sell_price'] = sell_price
                        results[-1]['profit_pct'] = profit_pct
                        results[-1]['duration'] = duration
                        break
                    
                    elif duration >= 90:
                        sell_price = prices.loc[sell_date, 'Close']
                        profit_pct = (sell_price - buy_price) / buy_price
                        duration = (pd.to_datetime(sell_date) - pd.to_datetime(date)).days
                        hold_list.remove(ticker)

                        results[-1]['sell_date'] = sell_date
                        results[-1]['sell_price'] = sell_price
                        results[-1]['profit_pct'] = profit_pct
                        results[-1]['duration'] = duration
                        break

    return pd.DataFrame(results)

# 결과를 엑셀로 저장

def save_results_to_excel(results, filename):
    """
    백테스트 결과를 엑셀 파일로 저장

    Parameters:
        results (pd.DataFrame): 백테스트 결과
        filename (str): 저장할 파일 이름

    Returns:
        None
    """
    results.to_excel(filename, index=False, encoding="utf-8-sig")
    print(f"결과가 {filename}에 저장되었습니다.")


def convert_datetime_string(date_str):
    # 문자열을 datetime 객체로 변환
    dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
    # datetime 객체를 원하는 형식의 문자열로 변환
    return dt.strftime('%Y%m%d')

def days_since_max_high(prices: pd.DataFrame, current_date: str, window_days: int = 600) -> int:
    """
    prices: 인덱스가 날짜 문자열('YYYY-MM-DD')인 DataFrame, 'High' 컬럼 보유
    current_date: 기준일 ('YYYY-MM-DD')
    window_days: 몇 거래일(window) 기준으로 볼지
    """
    # 기준일까지의 데이터 중 최근 window_days개
    window = prices.loc[:current_date].tail(window_days)
    if window.empty:
        return None
    # 최고가 발생일
    max_date = window['High'].idxmax()
    # 날짜 차이(일수)
    return (pd.to_datetime(current_date) - pd.to_datetime(max_date)).days

def fetch_index_close(date_str: str, market: str = 'KOSPI') -> float:
    """
    date_str: 'YYYYMMDD' 형식
    market: 'KOSPI' 또는 'KOSDAQ'
    
    pykrx.stock.get_index_ohlcv_by_date를 사용합니다.  
    """
    # 인덱스 코드 매핑 (1000: 코스피, 1001: 코스닥)
    code_map = {'KOSPI': '1001', 'KOSDAQ': '2001'}
    idx_code = code_map.get(market.upper(), '1000')
    df_idx = stock.get_index_ohlcv_by_date(date_str, date_str, idx_code)  # :contentReference[oaicite:0]{index=0}
    # 반환 컬럼: pykrx 버전에 따라 '종가' 혹은 'Close'
    if '종가' in df_idx.columns:
        return df_idx['종가'].iloc[-1]
    return df_idx['Close'].iloc[-1]


if __name__ == '__main__':
    root = "./sqlite3"
    
    # SQLite 데이터베이스 연결
    database_path = os.path.join(root, "screener.sqlite3")
    conn_scr = sqlite3.connect(database_path)

    cor_screener = pd.read_sql("SELECT * FROM 'cor.KS'", conn_scr, index_col='Date')
    vrate_screener = pd.read_sql("SELECT * FROM 'vrate.KS'", conn_scr, index_col='Date')
    mapct_screener = pd.read_sql("SELECT * FROM 'mapct.KS'", conn_scr, index_col='Date')

    database_path = os.path.join(root, "kr_stocklist.sqlite3")
    conn = sqlite3.connect(database_path)

    dfs = {}
    contents = []
    for date, mapct in mapct_screener.iterrows():
        ticker1 = set(mapct[mapct < 0].index)
        if len(ticker1) == 0:
            continue

        vrate = vrate_screener.loc[date]
        ticker2 = set(vrate[vrate > 8].index)
        if len(ticker2) == 0:
            continue
        
        cor = cor_screener.loc[date]
        ticker3 = set(cor[cor > 0.03].index)
        if len(ticker3) == 0:
            continue

        tickers = set.intersection(ticker1, ticker2, ticker3)
        if len(tickers) == 0:
            continue
        
        for ticker in tickers:
            if ticker not in dfs:
                dfs[ticker] = pd.read_sql(f"SELECT * FROM '{ticker}'", con=conn, index_col='Date')

        each = []
        for ticker in tickers:
            each.append([date, ticker, cor[ticker], vrate[ticker], mapct[ticker]])

        contents += each
        
    screener = pd.DataFrame(contents, columns=['Date', 'ticker', 'cor', 'vrate', 'ma200pct'])

    conn.close()
    conn_scr.close()

    df_result = run_backtest(root, screener, dfs)
    df_result.to_excel("results/results.xlsx", index=False)
