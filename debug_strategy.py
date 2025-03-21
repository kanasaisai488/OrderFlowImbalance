import pandas as pd
import numpy as np
import os
from engine.start import run_pipeline
from engine.strategy_executor import execute_linear_strategy
from engine.feature_engineer import compute_mid_price
from engine.data_loader import load_csv_data
from engine.utils import get_data_path

def debug_strategy():
    # Load a sample file
    data_path = get_data_path("databento", "RBJ5")
    sample_file = os.listdir(data_path)[0]
    file_path = os.path.join(data_path, sample_file)
    df = load_csv_data(file_path)

    # Run the pipeline to get model coefficients
    run_pipeline()

    # Load model coefficients
    coefs_df = pd.read_csv("model_coefficients_summary.csv", index_col=0)
    coefs = coefs_df.iloc[0]  # Use first file's coefficients

    # Execute strategy with debug prints
    print("\n=== Debugging Strategy Execution ===")
    strategy_result = execute_linear_strategy(df, coefs, lags=5, strategy='A', threshold=0.2)

    # Analyze results
    if not strategy_result['results']:
        print("❌ No trades executed. Check if efpc exceeds threshold.")
    else:
        trades_df = pd.DataFrame(strategy_result['results'])
        print("\n=== Trade Details ===")
        print(trades_df)
        
        total_trades = len(trades_df)
        winning_trades = len(trades_df[trades_df['pnl'] > 0])
        losing_trades = len(trades_df[trades_df['pnl'] < 0])
        
        print(f"\nTotal Trades: {total_trades}")
        print(f"Winning Trades: {winning_trades}")
        print(f"Losing Trades: {losing_trades}")
        print(f"Total PnL: {trades_df['pnl'].sum():.4f}")
        print(f"Average PnL per Trade: {trades_df['pnl'].mean():.4f}")

    # Check model coefficients
    print("\n=== Model Coefficients ===")
    print(coefs)

    # Check feature importance
    print("\n=== Feature Importance ===")
    print(coefs.sort_values(ascending=False))

if __name__ == "__main__":
    debug_strategy()
'''

To use this script:

1. Run `python debug_strategy.py` from the command line
2. It will:
   - Load sample data
   - Run the strategy pipeline
   - Analyze the strategy execution
   - Print key metrics including total PnL, number of trades, and feature importance

The output will help identify:
- Whether any trades are being executed
- The characteristics of the trades
- The importance of different features
- Whether the model is generating meaningful predictions

If no trades are being executed, check the model coefficients and consider lowering the threshold. If trades are being executed but resulting in zero PnL, examine the trade execution logic and price calculations.
'''