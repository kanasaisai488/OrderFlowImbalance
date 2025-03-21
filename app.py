import matplotlib
matplotlib.use('Agg')  # Set the Agg backend before importing pyplot
from flask import Flask, jsonify, render_template, request
import os
import re
from sklearn.linear_model import LinearRegression
from engine.utils import get_data_path
from engine.data_loader import load_csv_data
from engine.backtest import Backtest
from engine.target_builder import compute_future_price_change, compute_triple_barrier_labels
from engine.feature_engineer import compute_mid_price, compute_VOI, compute_OIR, compute_MPB
import numpy as np
import pandas as pd
# New imports for causal graphs
from pgmpy.estimators import PC
import networkx as nx

import matplotlib.pyplot as plt
import io
import base64


app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/run_strategy_range', methods=['GET'])
def run_strategy_range():
    start_date = request.args.get('start')
    end_date = request.args.get('end')
    lag = int(request.args.get('lag', 5))
    delay = int(request.args.get('delay', 20))
    threshold = float(request.args.get('threshold', 0.002))
    strategy = request.args.get('strategy', 'A')

    data_path = get_data_path("databento", "RBJ5")
    files = [f for f in os.listdir(data_path) if f.endswith(".csv") and start_date <= re.search(r'\d{4}-\d{2}-\d{2}', f).group() <= end_date]
    if not files:
        return jsonify({"error": "No files found for the selected date range"}), 404

    aggregated_results = {'trades': [], 'pnl_series': [], 'metrics': {'profit_factor': 0, 'sharpe_ratio': 0, 'drawdown': 0}}
    cumulative_pnl = 0
    for file in sorted(files):  # Sort to process dates sequentially
        file_path = os.path.join(data_path, file)
        df = load_csv_data(file_path)
        try:
            backtest = Backtest(df, lags=lag, delay=delay, threshold=threshold, strategy=strategy)
            results = backtest.get_results()
            metrics = backtest.get_performance_metrics()

            # Adjust PnL series for continuity across days
            for item in results['pnl_series']:
                item['pnl'] += cumulative_pnl
            cumulative_pnl = results['pnl_series'][-1]['pnl'] if results['pnl_series'] else cumulative_pnl

            aggregated_results['trades'].extend(results['trades'])
            aggregated_results['pnl_series'].extend(results['pnl_series'])

            # Aggregate metrics (simplified; you may want more sophisticated aggregation)
            aggregated_results['metrics']['profit_factor'] += metrics['profit_factor']
            aggregated_results['metrics']['sharpe_ratio'] += metrics['sharpe_ratio']
            aggregated_results['metrics']['drawdown'] = max(aggregated_results['metrics']['drawdown'], metrics['drawdown'])
        except Exception as e:
            return jsonify({"error": f"Error processing {file}: {str(e)}"}), 400

    # Average some metrics if needed
    num_days = len(files)
    if num_days > 0:
        aggregated_results['metrics']['profit_factor'] /= num_days
        aggregated_results['metrics']['sharpe_ratio'] /= num_days

    return jsonify(aggregated_results)

@app.route('/get_dates', methods=['GET'])
def get_dates():
    """Return a list of available dates from CSV filenames."""
    data_path = get_data_path("databento", "RBJ5")
    if not os.path.exists(data_path):
        return jsonify({"error": "Data folder not found"}), 404
    files = [f for f in os.listdir(data_path) if f.endswith(".csv")]
    dates = [re.search(r'\d{4}-\d{2}-\d{2}', f).group() for f in files if re.search(r'\d{4}-\d{2}-\d{2}', f)]
    if not dates:
        return jsonify({"error": "No CSV files found"}), 404
    return jsonify(sorted(set(dates)))

@app.route('/get_regression_data', methods=['GET'])
def get_regression_data():
    # Extract query parameters
    factor = request.args.get('factor')
    outcome = request.args.get('outcome')
    start_date = request.args.get('start')
    end_date = request.args.get('end')
    delay = int(request.args.get('delay', 20))  # Configurable delay, default to 20

    # Validate inputs
    if not all([factor, outcome, start_date, end_date]):
        return jsonify({"error": "Missing parameters"}), 400

    # Load data files
    data_path = get_data_path("databento", "RBJ5")
    files = [f for f in os.listdir(data_path) 
             if f.endswith(".csv") and start_date <= re.search(r'\d{4}-\d{2}-\d{2}', f).group() <= end_date]
    if not files:
        return jsonify({"error": "No files found for the selected date range"}), 404

    # Aggregate data across files
    all_features = []
    all_outcomes = []
    for file in sorted(files):
        file_path = os.path.join(data_path, file)
        df = load_csv_data(file_path)
        mid_price = compute_mid_price(df)

        # Compute selected factor
        if factor == 'VOI':
            feature = compute_VOI(df)
        elif factor == 'OIR':
            feature = compute_OIR(df)
        elif factor == 'MPB':
            d_vol = df['Volume_x'].diff().replace(0, np.nan)
            d_turnover = df['Turnover'].diff()
            avg_trade_price = (d_turnover / d_vol) / 10000
            avg_trade_price.replace([np.inf, -np.inf], np.nan, inplace=True)
            avg_trade_price = avg_trade_price.bfill().ffill()
            feature = compute_MPB(avg_trade_price, mid_price)
        else:
            return jsonify({"error": "Invalid factor"}), 400

        # Compute selected outcome with configurable delay
        if outcome == 'return':
            outcome_series = compute_future_price_change(mid_price, delay=delay)
        elif outcome == 'triple_barrier':
            # Define fixed thresholds (can be made configurable later)
            profit_threshold = 0.002  # 0.2%
            stop_loss_threshold = 0.002  # 0.2%
            outcome_series = compute_triple_barrier_labels(mid_price, delay, profit_threshold, stop_loss_threshold)
        else:
            return jsonify({"error": "Invalid outcome"}), 400

        # Align and clean data
        aligned = pd.concat([feature, outcome_series], axis=1).dropna()
        all_features.extend(aligned.iloc[:, 0].values)
        all_outcomes.extend(aligned.iloc[:, 1].values)

    if not all_features:
        return jsonify({"error": "No valid data for regression"}), 404

    # Perform linear regression
    X = np.array(all_features).reshape(-1, 1)
    y = np.array(all_outcomes)
    model = LinearRegression()
    model.fit(X, y)
    line = model.predict(X)

    return jsonify({
        'x': all_features,
        'y': all_outcomes,
        'line': line.tolist()
    })
@app.route('/run_strategy/<date>', methods=['GET'])
def run_strategy(date):
    # Get query parameters with defaults
    lag = int(request.args.get('lag', 5))
    delay = int(request.args.get('delay', 20))
    threshold = float(request.args.get('threshold', 0.002))
    strategy = request.args.get('strategy', 'A')

    # Load data (example logic)
    data_path = get_data_path("databento", "RBJ5")
    file_name = f"rbj5_{date}.csv"
    file_path = os.path.join(data_path, file_name)
    if not os.path.exists(file_path):
        return jsonify({"error": "File not found"}), 404
    
    df = load_csv_data(file_path)
    try:
        # Run backtest with user-provided parameters
        backtest = Backtest(df, lags=lag, delay=delay, threshold=threshold, strategy=strategy)
        results = backtest.get_results()
        metrics = backtest.get_performance_metrics()
        return jsonify({
            'trades': results['trades'],
            'pnl_series': results['pnl_series'],
            'metrics': metrics
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route('/get_causal_graph', methods=['GET'])
def get_causal_graph():
    # Extract query parameters
    factors = request.args.getlist('factors')
    outcome = request.args.get('outcome')
    start_date = request.args.get('start')
    end_date = request.args.get('end')
    delay = int(request.args.get('delay', 20))

    # Validate inputs
    if not factors or not outcome or not start_date or not end_date:
        return jsonify({"error": "Missing parameters"}), 400

    # Load data files
    data_path = get_data_path("databento", "RBJ5")
    files = [f for f in os.listdir(data_path) 
             if f.endswith(".csv") and start_date <= re.search(r'\d{4}-\d{2}-\d{2}', f).group() <= end_date]
    if not files:
        return jsonify({"error": "No files found for the selected date range"}), 404

    # Aggregate data across files
    all_data = []
    for file in sorted(files):
        file_path = os.path.join(data_path, file)
        df = load_csv_data(file_path)
        mid_price = compute_mid_price(df)

        # Compute factors
        factor_series = {}
        for factor in factors:
            if factor == 'VOI':
                factor_series[factor] = compute_VOI(df)
            elif factor == 'OIR':
                factor_series[factor] = compute_OIR(df)
            elif factor == 'MPB':
                d_vol = df['Volume_x'].diff().replace(0, np.nan)
                d_turnover = df['Turnover'].diff()
                avg_trade_price = (d_turnover / d_vol) / 10000
                avg_trade_price.replace([np.inf, -np.inf], np.nan, inplace=True)
                avg_trade_price = avg_trade_price.bfill().ffill()
                factor_series[factor] = compute_MPB(avg_trade_price, mid_price)
            else:
                return jsonify({"error": f"Invalid factor: {factor}"}), 400

        # Compute outcome
        if outcome == 'return':
            outcome_series = compute_future_price_change(mid_price, delay=delay)
        elif outcome == 'triple_barrier':
            profit_threshold = 0.002
            stop_loss_threshold = 0.002
            outcome_series = compute_triple_barrier_labels(mid_price, delay, profit_threshold, stop_loss_threshold)
        else:
            return jsonify({"error": "Invalid outcome"}), 400

        # Align and clean data
        aligned = pd.concat([factor_series[f] for f in factors] + [outcome_series], axis=1)
        aligned.columns = factors + [outcome]
        aligned = aligned.dropna()
        all_data.append(aligned)

    # Combine data from all files
    full_data = pd.concat(all_data, ignore_index=True)
    if full_data.empty:
        return jsonify({"error": "No valid data for causal graph"}), 404

    # Estimate causal graph using PC algorithm
    estimator = PC(full_data)
    model = estimator.estimate()

    # Convert to NetworkX graph for visualization
    nx_graph = nx.DiGraph()
    nx_graph.add_nodes_from(model.nodes())
    nx_graph.add_edges_from(model.edges())

    # Generate graph image with error handling
    try:
        plt.figure(figsize=(10, 8))
        pos = nx.spring_layout(nx_graph)
        nx.draw(nx_graph, pos, with_labels=True, node_color='lightblue', node_size=3000, font_size=12, font_weight='bold')
        plt.title("Causal Graph")
        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)
        img_str = base64.b64encode(buf.read()).decode('utf-8')
        buf.close()
        plt.close()  # Free resources
    except Exception as e:
        return jsonify({"error": f"Failed to generate graph: {str(e)}"}), 500

    return jsonify({'graph_image': img_str})

if __name__ == '__main__':
    app.run(debug=True)