import pandas as pd
import numpy as np
from engine.feature_engineer import compute_mid_price, compute_spread, compute_OIR, compute_VOI, compute_MPB, compute_lags
from engine.target_builder import compute_future_price_change
from engine.model_builder import fit_linear_model

class Backtest:
    def __init__(self, data, lags=5, delay=20, threshold=0.002, strategy='A'):
        self.data = data
        self.lags = lags
        self.delay = delay
        self.threshold = threshold
        self.strategy = strategy
        self.features, self.y = self._prepare_features_and_target()
        self.model, self.coefs = self._fit_model()
        self.trades, self.pnl_series = self._execute_strategy()

    def _prepare_features_and_target(self):
        mid_price = compute_mid_price(self.data)
        spread = compute_spread(self.data)
        oir = compute_OIR(self.data)
        voi = compute_VOI(self.data)
        d_vol = self.data['Volume_x'].diff().replace(0, np.nan)
        d_turnover = self.data['Turnover'].diff()
        avg_trade_price = (d_turnover / d_vol) / 10000
        avg_trade_price.replace([np.inf, -np.inf], np.nan, inplace=True)
        avg_trade_price = avg_trade_price.bfill().ffill()
        mpb = compute_MPB(avg_trade_price, mid_price)
        voi_lags = compute_lags(voi, lags=self.lags, prefix='VOI')
        oir_lags = compute_lags(oir, lags=self.lags, prefix='OIR')
        mpb_lags = compute_lags(mpb, lags=self.lags, prefix='MPB')
        if self.strategy == 'A':
            features = pd.concat([voi_lags, oir_lags, mpb_lags], axis=1).dropna()
        elif self.strategy == 'B':
            features = pd.concat([voi_lags, oir_lags], axis=1).dropna()
        else:
            raise ValueError("Invalid strategy. Choose 'A' or 'B'.")
        y = compute_future_price_change(mid_price, delay=self.delay)
        y = y.loc[features.index]
        features, y = features[~y.isna()], y.dropna()
        return features, y

    def _fit_model(self):
        if len(self.features) <= 50:
            raise ValueError("Not enough data to fit model")
        model, coefs = fit_linear_model(self.features, self.y)
        return model, coefs

    def _execute_strategy(self):
        mid_price = compute_mid_price(self.data)
        time_secs = self.data['SecondOfDay'] + self.data['UpdateMillisec'] / 1000
        ask = self.data['AskPrice1']
        bid = self.data['BidPrice1']
        results = []
        trades = []
        own = False
        pos = 0
        entry_price = 0
        entry_time = 0
        pnl = 0
        trade_cost_per_unit = 0.00005  # 0.005% per trade
        for t in self.features.index:
            x = self.features.loc[t].values
            efpc = np.dot(x, self.coefs.values)
            ask_t = ask.loc[t]
            bid_t = bid.loc[t]
            time_t = time_secs.loc[t]
            mid_price_t = mid_price.loc[t]
            bid_price_t = bid.loc[t]
            ask_price_t = ask.loc[t]
            action = 'none'
            trade_pnl = 0
            trade_cost = 0
            if not own and efpc > self.threshold:
                action = 'buy'
                own = True
                pos = 1
                entry_price = ask_t
                entry_time = time_t
                trade_cost = entry_price * trade_cost_per_unit
                pnl -= trade_cost
            elif own and efpc < -self.threshold:
                action = 'sell'
                own = False
                pos = 0
                trade_pnl = bid_t - entry_price
                trade_cost = bid_t * trade_cost_per_unit
                pnl += trade_pnl - trade_cost
                trades.append({
                    'open_time': entry_time,
                    'open_price': entry_price,
                    'close_time': time_t,
                    'close_price': bid_t,
                    'pnl': trade_pnl - trade_cost
                })
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
        return trades, results

    def get_results(self):
        return {
            'trades': self.trades,
            'pnl_series': self.pnl_series
        }

    def get_performance_metrics(self):
        trades_df = pd.DataFrame(self.trades)
        if trades_df.empty:
            return {
                'profit_factor': 0,
                'sharpe_ratio': 0,
                'drawdown': 0
            }
        total_pnl = trades_df['pnl'].sum()
        profit_factor = total_pnl / abs(trades_df[trades_df['pnl'] < 0]['pnl'].sum()) if len(trades_df[trades_df['pnl'] < 0]) > 0 else np.inf
        sharpe_ratio = (trades_df['pnl'].mean() / trades_df['pnl'].std()) * np.sqrt(252) if trades_df['pnl'].std() != 0 else 0
        cumulative_pnl = trades_df['pnl'].cumsum()
        drawdown = (cumulative_pnl.cummax() - cumulative_pnl).max()
        return {
            'profit_factor': profit_factor,
            'sharpe_ratio': sharpe_ratio,
            'drawdown': drawdown
        }