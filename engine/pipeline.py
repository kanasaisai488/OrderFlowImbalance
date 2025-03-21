# engine/pipeline.py
import pandas as pd
import numpy as np
from engine.data_loader import load_csv_data
from engine.feature_engineer import *
from engine.target_builder import compute_future_price_change
from engine.model_builder import fit_linear_model
from engine.strategy_executor import execute_linear_strategy

def run_pipeline_single_file(file_path, lags=5, delay=20, threshold=0.002):
    """
    Process a single CSV file and return strategy results, consistent with original run_pipeline.
    """
    try:
        df = load_csv_data(file_path)

        mid_price = compute_mid_price(df)
        spread = compute_spread(df)
        oir = compute_OIR(df)
        voi = compute_VOI(df)

        d_vol = df['Volume_x'].diff().replace(0, np.nan)
        d_turnover = df['Turnover'].diff()
        avg_trade_price = (d_turnover / d_vol) / 10000
        avg_trade_price.replace([np.inf, -np.inf], np.nan, inplace=True)
        avg_trade_price = avg_trade_price.bfill().ffill()
        mpb = compute_MPB(avg_trade_price, mid_price)

        voi_lags = compute_lags(voi, lags=lags, prefix='VOI')
        oir_lags = compute_lags(oir, lags=lags, prefix='OIR')
        mpb_lags = compute_lags(mpb, lags=lags, prefix='MPB')

        features = pd.concat([voi_lags, oir_lags, mpb_lags], axis=1).dropna()
        y = compute_future_price_change(mid_price, delay=delay)
        y = y.loc[features.index]
        features, y = features[~y.isna()], y.dropna()

        if len(features) <= 50:
            return {"error": "Not enough data to fit model"}

        model, coefs = fit_linear_model(features, y)
        strategy_result = execute_linear_strategy(df, coefs, lags=lags, strategy='A', threshold=threshold)
        return strategy_result

    except Exception as e:
        return {"error": f"Failed to process file: {e}"}