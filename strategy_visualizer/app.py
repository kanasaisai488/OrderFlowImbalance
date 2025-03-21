#strategy_visualizer/app.py

from flask import Flask, jsonify, render_template
import os
import re
from engine.utils import get_data_path
from engine.pipeline import run_pipeline_single_file

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

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

@app.route('/run_strategy/<date>', methods=['GET'])
def run_strategy(date):
    """Run the strategy for a selected date and return results as JSON."""
    data_path = get_data_path("databento", "RBJ5")
    file_name = f"rbj5_{date}.csv"  # Matches your file naming convention
    file_path = os.path.join(data_path, file_name)

    if not os.path.exists(file_path):
        return jsonify({"error": "File not found"}), 404

    result = run_pipeline_single_file(file_path, lags=5, delay=20, threshold=0.002)
    if "error" in result:
        return jsonify(result), 400

    return jsonify(result['results'])

if __name__ == '__main__':
    app.run(debug=True)