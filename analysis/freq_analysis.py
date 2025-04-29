import numpy as np
import pandas as pd
from pykrx import stock
# 1) EMD 기반 밴드패스
from PyEMD import EMD

import matplotlib.pyplot as plt

def plot_bands_with_original(dates, band_emd, band_wave, original_series):
    """
    dates:    datetime 인덱스 또는 리스트
    band_emd: EMD 기반 밴드시계열 ([-1,1] 스케일)
    band_wave: 웨이블릿 기반 밴드시계열 ([-1,1] 스케일)
    original_series: 원본 지수(예: kospi_index)
    """
    fig, ax1 = plt.subplots(figsize=(12, 5))

    # 좌측 축: 두 밴드 시그널
    ax1.plot(dates, band_emd, label="EMD Band", linestyle='-')
    ax1.plot(dates, band_wave, label="Wavelet Band", linestyle='--')
    ax1.set_ylabel("Band Signal (scaled to [-1,1])")
    ax1.set_ylim(-1.1, 1.1)
    ax1.legend(loc="upper left")

    # 우측 축: 원본 지수
    ax2 = ax1.twinx()
    ax2.plot(dates, original_series, label="Original Index", alpha=0.6)
    ax2.set_ylabel("Original Index Value")
    ax2.legend(loc="upper right")

    ax1.set_title("EMD vs Wavelet Bandpass with Original Index")
    ax1.set_xlabel("Date")
    plt.tight_layout()
    plt.show()

def get_kospi_index_series(start_date: str, end_date: str) -> pd.Series:
    """
    주어진 기간의 코스피 지수 종가를 반환합니다.
    
    Parameters:
        start_date (str): 조회 시작일, 'YYYYMMDD' 형식
        end_date   (str): 조회 종료일, 'YYYYMMDD' 형식
    
    Returns:
        pd.Series: 인덱스가 datetime, 값이 종가인 시계열
    """
    # pykrx에서 코스피 지수 코드는 '1001'
    df = stock.get_index_ohlcv_by_date(start_date, end_date, "1001")
    
    # 컬럼명이 '종가'인지 'Close'인지 판정
    price_col = '종가' if '종가' in df.columns else 'Close'
    
    # Series로 변환하고 인덱스를 datetime으로 지정
    ser = df[price_col].copy()
    ser.index = pd.to_datetime(ser.index, format="%Y-%m-%d")
    
    return ser

def band_via_emd(series: pd.Series, 
                 imf_idxs: tuple) -> (np.ndarray, np.ndarray):
    """
    series: pandas.Series (index: 날짜, values: 시계열 값)
    remove_imfs: (high_freq_idx, low_freq_idx) 로, 
                 - high_freq_idx=0 이면 첫 번째 IMF(가장 고주파) 제거
                 - low_freq_idx=-1 이면 마지막 IMF(DC/trend) 제거
    returns:
        band: 선택된 IMF들을 합성한 밴드시계열 (scaled to [-1,1])
        imfs: 전체 IMF 배열 shape=(n_imfs, n_samples)
    """
    # 1) EMD 분해
    emd = EMD()
    imfs = emd(series.values)      # shape = (n_imfs, len(series))
    print("# imfs: ", len(imfs))

    # 고주파, 저주파 각각 제거
    band = imfs[imf_idxs, :].sum(axis=0)

    # 3) [-1, 1] 스케일링
    band_norm = 2 * (band - band.min()) / (band.max() - band.min()) - 1

    return band_norm, imfs


# 2) 웨이블릿 기반 밴드패스 ------------------------------------------------
import pywt

def band_via_wavelet(series: pd.Series,
                     wavelet: str = 'db4',
                     level: int = 5,
                     keep_levels: list = None) -> np.ndarray:
    """
    series: pandas.Series
    wavelet: 웨이블릿 종류 (예: 'db4', 'sym5' 등)
    level: 최대 분해 레벨
    keep_levels: 남길 디테일 계수 레벨 리스트 (1이 가장 고주파)
                 예) [2,3] 은 너무 고주파(1)과 너무 저주파(>3)를 제외
    returns:
        band_norm: 선택 계수만 재구성한 밴드시계열 (scaled to [-1,1])
    """
    # 1) 다중 레벨 분해
    coeffs = pywt.wavedec(series.values, wavelet=wavelet, level=level)
    # coeffs = [cA_n, cD_n, cD_{n-1}, ..., cD_1]

    # 2) 제외할 수준(기본: 제외 없음 → 모두 사용)
    if keep_levels is None:
        # 기본: 1~level-1 (즉 DC(cA_n)와 최상위 디테일(cD_1)은 제거)
        keep_levels = list(range(2, level))
    # 다시 재구성을 위해 DC는 coeffs[0]=cA_n, 디테일은 coeffs[1]→cD_n...coeffs[-1]=cD_1
    new_coeffs = [np.zeros_like(coeffs[0])]  # DC 성분 빼려면 0으로
    for i in range(1, len(coeffs)):
        lvl = level - (i - 1)
        new_coeffs.append(coeffs[i] if lvl in keep_levels else np.zeros_like(coeffs[i]))

    # 3) 역변환
    band = pywt.waverec(new_coeffs, wavelet=wavelet)

    # 4) 길이 맞추기 (padding/trim)
    band = band[: len(series)]

    # 5) [-1,1] 스케일링
    band_norm = 2 * (band - band.min()) / (band.max() - band.min()) - 1

    return band_norm


kospi_index = get_kospi_index_series("20220101", "20250423")
# kospi_index: pd.Series
band_emd, imfs = band_via_emd(kospi_index, imf_idxs=(4, 5))
band_wave = band_via_wavelet(kospi_index, wavelet='db4', level=6, keep_levels=[2,3,4])

dates = kospi_index.index
plot_bands_with_original(dates, band_emd, band_wave, kospi_index.values)
