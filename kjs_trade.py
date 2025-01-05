import sqlite3
import pandas as pd
from datetime import datetime
from pykrx import stock


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

def run_backtest(screener_data, dfs, n_split=4):
    """
    백테스트 실행

    Parameters:
        screener_data (pd.DataFrame): 선정된 종목 데이터 (Date, ticker)
        price_data (pd.DataFrame): 종목 가격 데이터
        seed (float): 초기 투자 금액

    Returns:
        pd.DataFrame: 백테스트 결과
    """
    database_path = "fundamental.sqlite3"
    conn_fund = sqlite3.connect(database_path)

    results = []
    hold_list = []
    for date, each in screener_data.groupby('Date'):
        df_fund = pd.read_sql(f"SELECT * FROM '{convert_datetime_string(date)}'", conn_fund, index_col='티커')

        for _, row in each.iterrows():
            ticker = row['ticker']
            cor = row['cor']
            vrate = row['vrate']
            mapct = row['mapct']

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
                    **fundamental.to_dict()
                })

                for sell_date, each in next_prices.iterrows():
                    duration = (pd.to_datetime(sell_date) - pd.to_datetime(date)).days

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
                    
                    elif duration >= 600:
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


if __name__ == '__main__':
    # SQLite 데이터베이스 연결
    database_path = "screener.sqlite3"
    conn_scr = sqlite3.connect(database_path)

    cor_screener = pd.read_sql("SELECT * FROM 'cor.KS'", conn_scr, index_col='Date')
    vrate_screener = pd.read_sql("SELECT * FROM 'vrate.KS'", conn_scr, index_col='Date')
    mapct_screener = pd.read_sql("SELECT * FROM 'mapct.KS'", conn_scr, index_col='Date')

    database_path = "kr_stocklist.sqlite3"
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
        
    screener = pd.DataFrame(contents, columns=['Date', 'ticker', 'cor', 'vrate', 'mapct'])

    conn.close()
    conn_scr.close()

    df_result = run_backtest(screener, dfs)
    df_result.to_excel("results/results.xlsx", index=False)
