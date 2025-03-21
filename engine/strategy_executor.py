# engine/strategy_executor.py
import pandas as pd
import numpy as np
from engine.feature_engineer import compute_mid_price, compute_spread, compute_OIR, compute_VOI, compute_MPB, compute_lags

def execute_linear_strategy(data, coefs, lags=5, strategy='A', threshold=0.2, trade_at_mid=False):
    """
    Executes a linear trading strategy based on model coefficients, including price data in results.
    """
    # Feature Engineering (consistent with your original code)
    mid_price = compute_mid_price(data)
    spread = compute_spread(data)
    oir = compute_OIR(data)
    voi = compute_VOI(data)

    d_vol = data['Volume_x'].diff().replace(0, np.nan)
    d_turnover = data['Turnover'].diff()
    avg_trade_price = (d_turnover / d_vol) / 10000
    avg_trade_price.replace([np.inf, -np.inf], np.nan, inplace=True)
    avg_trade_price = avg_trade_price.bfill().ffill()
    mpb = compute_MPB(avg_trade_price, mid_price)

    voi_lags = compute_lags(voi, lags=lags, prefix='VOI')
    oir_lags = compute_lags(oir, lags=lags, prefix='OIR')
    mpb_lags = compute_lags(mpb, lags=lags, prefix='MPB')

    if strategy == 'A':
        features = pd.concat([voi_lags, oir_lags, mpb_lags], axis=1).dropna()
    elif strategy == 'B':
        features = pd.concat([voi_lags, oir_lags], axis=1).dropna()
    else:
        raise ValueError("Invalid strategy. Choose 'A' or 'B'.")

    time_secs = data['SecondOfDay'] + data['UpdateMillisec'] / 1000
    ask = mid_price if trade_at_mid else data['AskPrice1']
    bid = mid_price if trade_at_mid else data['BidPrice1']

    # Strategy Execution
    results = []
    own = False
    pos = 0
    entry_price = 0
    pnl = 0
    trade_cost_per_unit = 0.00005  # 0.005% per trade

    for t in features.index:
        x = features.loc[t].values
        efpc = np.dot(x, coefs.values)
        ask_t = ask.loc[t]
        bid_t = bid.loc[t]
        time_t = time_secs.loc[t]
        mid_price_t = mid_price.loc[t]
        bid_price_t = data['BidPrice1'].loc[t]
        ask_price_t = data['AskPrice1'].loc[t]

        action = 'none'
        trade_pnl = 0
        trade_cost = 0

        if not own and efpc > threshold:
            action = 'buy'
            own = True
            pos = 1
            entry_price = ask_t
            trade_cost = entry_price * trade_cost_per_unit
            pnl -= trade_cost
        elif own and efpc < -threshold:
            action = 'sell'
            own = False
            pos = 0
            trade_pnl = bid_t - entry_price
            trade_cost = bid_t * trade_cost_per_unit
            pnl += trade_pnl - trade_cost
            entry_price = 0

        results.append({
            'time': time_t,
            'pnl': pnl,
            'strategy': action,
            'trade.pnl': trade_pnl,
            'trade.costs': trade_cost,
            'mid_price': mid_price_t,
            'bid_price': bid_price_t,
            'ask_price': ask_price_t
        })

    return {'results': results}