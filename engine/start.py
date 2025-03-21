# engine/start.py

import os
import pandas as pd
from engine.data_loader import load_csv_data
from engine.feature_engineer import *
from engine.target_builder import compute_future_price_change
from engine.model_builder import fit_linear_model
from engine.utils import get_data_path
from engine.strategy_executor import execute_linear_strategy # Import the new function

def run_pipeline():
    data_path = get_data_path("databento", "RBJ5")

    if not os.path.exists(data_path):
        raise FileNotFoundError(f"Data folder not found: {data_path}")

    files = [f for f in os.listdir(data_path) if f.endswith(".csv")]
    if not files:
        raise FileNotFoundError("No CSV files found in the data folder")

    results = []
    strategy_results = []

    for file in files:
        file_path = os.path.join(data_path, file)
        print(f"Processing: {file_path}")

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

            voi_lags = compute_lags(voi, lags=5, prefix='VOI')
            oir_lags = compute_lags(oir, lags=5, prefix='OIR')
            mpb_lags = compute_lags(mpb, lags=5, prefix='MPB')

            features = pd.concat([voi_lags, oir_lags, mpb_lags], axis=1).dropna()
            y = compute_future_price_change(mid_price, delay=20)
            y = y.loc[features.index]
            features, y = features[~y.isna()], y.dropna()

            if len(features) > 50:
                model, coefs = fit_linear_model(features, y)
                coefs.name = file.replace('.csv', '')
                results.append(coefs)

                # Execute the strategy and collect results
                strategy_result = execute_linear_strategy(df, coefs, lags=5, strategy='A', threshold=0.002)
                # Assuming strategy_result is a dict with a 'results' key containing a list of trade details
                strategy_results.append(pd.DataFrame(strategy_result['results']))
                

        except Exception as e:
            print(f"❌ Failed to process {file}: {e}")

    if results:
        result_df = pd.DataFrame(results)
        print("\n=== Model Coefficients ===")
        print(result_df)
        result_df.to_csv("model_coefficients_summary.csv")

        # Combine and save strategy results
        if strategy_results:  # Check if there are any strategy results
            combined_strategy_results = pd.concat(strategy_results, ignore_index=True)
            combined_strategy_results.to_csv("strategy_results.csv", index=False)
            print("Strategy results saved to strategy_results.csv")
        else:
            print("No strategy results were generated.")
    else:
        print("No models were successfully fitted.")



#Single Date Pipeline Retrieval
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
