#engine/feature_engineer.py
import numpy as np
import pandas as pd

def compute_mid_price(df):
    return (df['BidPrice1'] + df['AskPrice1']) / 2

def compute_spread(df):
    spread = df['AskPrice1'] - df['BidPrice1']
    spread[spread == 0] = 1e-5  # Default tick price
    return spread

def compute_OIR(df):
    return (df['BidVolume1'] - df['AskVolume1']) / (df['BidVolume1'] + df['AskVolume1'])

def compute_VOI(df):
    d_bid = df['BidPrice1'].diff().fillna(0)
    d_ask = df['AskPrice1'].diff().fillna(0)
    bid_cv = (df['BidVolume1'] - df['BidVolume1'].shift(1).fillna(0)) * (d_bid >= 0).astype(int)
    ask_cv = (df['AskVolume1'] - df['AskVolume1'].shift(1).fillna(0)) * (d_ask <= 0).astype(int)
    return (bid_cv - ask_cv).fillna(0)

def compute_MPB(avg_trade_price, mid_price):
    return avg_trade_price - mid_price.rolling(2).mean().shift(1).fillna(mid_price)

def compute_lags(series, lags=5, prefix=''):
    df = pd.DataFrame({f'{prefix}.t0': series})
    for i in range(1, lags + 1):
        df[f'{prefix}.t{i}'] = series.shift(i)
    return df
