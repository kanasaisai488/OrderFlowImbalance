#engine/target_builder.py

import numpy as np
import pandas as pd
def compute_future_price_change(mid_price, delay=20):
    avg_future_price = mid_price.rolling(delay).mean().shift(-delay + 1)
    dMid_Response = avg_future_price - mid_price
    return dMid_Response

def compute_triple_barrier_labels(price_series, delay, profit_threshold, stop_loss_threshold):
    """
    Compute labels using the triple barrier method.
    
    Parameters:
    - price_series (pd.Series): Time series of prices (e.g., mid prices).
    - delay (int): Number of future time steps to consider (time barrier).
    - profit_threshold (float): Fractional price increase to trigger profit-taking (e.g., 0.002 for 0.2%).
    - stop_loss_threshold (float): Fractional price decrease to trigger stop-loss (e.g., 0.002 for 0.2%).
    
    Returns:
    - pd.Series: Labels (+1, -1, 0) aligned with the price series index.
    """
    labels = []
    for i in range(len(price_series) - delay):
        current_price = price_series.iloc[i]
        future_prices = price_series.iloc[i:i + delay]
        
        # Check if barriers are hit within the delay period
        profit_hit = any(future_prices >= current_price * (1 + profit_threshold))
        stop_loss_hit = any(future_prices <= current_price * (1 - stop_loss_threshold))
        
        if profit_hit and stop_loss_hit:
            # Determine which barrier was hit first
            profit_index = next((idx for idx, val in enumerate(future_prices) 
                               if val >= current_price * (1 + profit_threshold)), None)
            stop_loss_index = next((idx for idx, val in enumerate(future_prices) 
                                  if val <= current_price * (1 - stop_loss_threshold)), None)
            if profit_index < stop_loss_index:
                labels.append(1)
            else:
                labels.append(-1)
        elif profit_hit:
            labels.append(1)
        elif stop_loss_hit:
            labels.append(-1)
        else:
            labels.append(0)
    
    # Pad the end with NaN where future prices aren’t available
    labels.extend([np.nan] * delay)
    return pd.Series(labels, index=price_series.index)
