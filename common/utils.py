import pandas as pd
import yaml

def load_yaml(file_path):
    """
    Reads a YAML file and returns its contents as a Python dictionary.

    Parameters:
        file_path (str): The path to the YAML file.

    Returns:
        dict: The contents of the YAML file.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            # Use safe_load to prevent execution of arbitrary code
            data = yaml.safe_load(file)
        return data
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
    except yaml.YAMLError as exc:
        print(f"Error parsing YAML file: {exc}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def getStockCode(market):
    if market == 'kosdaq':
        url_market = 'kosdaqMkt'
    elif market == 'kospi':
        url_market = 'stockMkt'
    else:
        raise ValueError('invalid market ')

    url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=%s' % url_market
    df = pd.read_html(url, encoding='euc-kr', header=0)[0]
    return df.assign(종목코드=df['종목코드'].astype(str).str.zfill(6))

def getAllStockCode():
    return pd.concat(
        [getStockCode(market).assign(type=code) 
            for market, code in zip(['kosdaq', 'kospi'], ['KQ', 'KS'])]
    )
