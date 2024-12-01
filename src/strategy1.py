# strategy1.py

import pandas as pd
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from config.config import (
    STRATEGY_1, BUY_AMOUNTS, CAPTURE_STRATEGY1_DATA, TICKERS,
    DB_FILE, SCENARIO
)
from trade_executor import place_order
from risk_manager import check_risk_management
from db_manager import log_strategy1_data, log_error
from indicators import calculate_indicators
import threading
import schedule
import sqlite3

# Configure logging
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOGS_DIR = os.path.join(BASE_DIR, 'logs')
if not os.path.exists(LOGS_DIR):
    os.makedirs(LOGS_DIR)

from logging.handlers import RotatingFileHandler

log_file = os.path.join(LOGS_DIR, 'strategy1.log')
logger = logging.getLogger('strategy1')
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(
    log_file, maxBytes=5 * 1024 * 1024, backupCount=5
)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(handler)

def strategy1(df, ticker):
    """
    Apply Strategy 1 to the DataFrame.

    :param df: DataFrame with indicators calculated
    :param ticker: str, the trading pair
    """
    # Limit DataFrame size to prevent memory issues
    df = df.tail(500)  # Keep only the last 500 rows

    # Check if df is None or empty
    if df is None or df.empty:
        logger.warning(f"No data available for {ticker} in Strategy 1.")
        return

    # Verify that required indicators are in the DataFrame
    required_indicators = [
        'PSAR_trend',
        'STOCH_K_9_3', 'STOCH_K_14_3', 'STOCH_K_40_4', 'STOCH_K_60_10_1',
        'BB_percent_b'
    ]
    if not all(indicator in df.columns for indicator in required_indicators):
        logger.error(f"Missing indicators for {ticker} in Strategy 1.")
        return

    # Capture data if enabled
    if CAPTURE_STRATEGY1_DATA:
        latest_row = df.iloc[-1]
        data = latest_row.to_dict()
        data['ticker'] = ticker
        data['timestamp'] = latest_row['timestamp'].isoformat()
        log_strategy1_data(data)
        logger.info(f"Captured data for {ticker} at {data['timestamp']}")

    # Check risk management conditions
    if not check_risk_management('Strategy 1'):
        return

    # Get the last row (most recent data)
    last_row = df.iloc[-1]

    # Add a timestamp check
    data_timestamp = pd.to_datetime(last_row['timestamp'], utc=True)
    current_time = datetime.now(timezone.utc)

    # Ignore data points older than a certain threshold (e.g., 5 minutes)
    if (current_time - data_timestamp).total_seconds() > 300:
        logger.info(f"Ignoring old data for {ticker}.")
        return

    # Check Parabolic SAR conditions
    psar_trend = last_row.get('PSAR_trend')
    psar_prev_trend = df.iloc[-2].get('PSAR_trend') if len(df) > 1 else None
    psar_swing_to_long = (psar_prev_trend == -1) and (psar_trend == 1)

    if not psar_swing_to_long:
        return

    # Check Stochastic conditions
    stoch_conditions = all([
        last_row.get('STOCH_K_9_3', 100) < STRATEGY_1['stochastic_levels']['STOCH_K_9_3'],
        last_row.get('STOCH_K_14_3', 100) < STRATEGY_1['stochastic_levels']['STOCH_K_14_3'],
        last_row.get('STOCH_K_40_4', 100) < STRATEGY_1['stochastic_levels']['STOCH_K_40_4'],
        last_row.get('STOCH_K_60_10_1', 100) < STRATEGY_1['stochastic_levels']['STOCH_K_60_10_1'],
    ])

    if not stoch_conditions:
        return

    # Check Bollinger Bands condition
    if last_row.get('BB_percent_b', 100) >= STRATEGY_1['bb_percent_b_threshold']:
        return

    # All conditions met, place a buy order
    logger.info(f"Strategy 1 conditions met for {ticker}. Placing buy order.")
    amount = BUY_AMOUNTS.get(ticker, STRATEGY_1['default_buy_amount'])
    place_order(
        ticker=ticker,
        side='BUY',
        amount=amount,
        strategy='Strategy 1',
        order_type='limit',
        time_in_force='GTC',
        make_order=True  # Ensures it's a maker order
    )

def fetch_and_execute_strategy1(ticker, scenario=SCENARIO):
    """
    Fetch data and execute Strategy 1.

    :param ticker: str, the trading pair
    :param scenario: str, 'A' or 'B'
    """
    try:
        conn = sqlite3.connect(DB_FILE)
        df = pd.read_sql_query(f"SELECT * FROM candlesticks_{ticker.replace('-', '_')} ORDER BY timestamp ASC", conn)
        conn.close()

        if df.empty:
            logger.warning(f"No data for {ticker}. Skipping Strategy 1.")
            return

        df['timestamp'] = pd.to_datetime(df['timestamp'])

        # Calculate indicators
        df = calculate_indicators(df)

        # Execute the strategy
        strategy1(df, ticker)
    except Exception as e:
        logger.error(f"Error in Strategy 1 for {ticker}: {e}")
        log_error(str(e))

def schedule_strategy1(scenario=SCENARIO):
    """
    Schedule Strategy 1 to run at specified times.
    For Scenario A, execute immediately.
    For Scenario B, schedule at specific times.
    """
    tickers = TICKERS  # Define your tickers list in config

    if scenario == 'A':
        # Scenario A: Execute immediately and then every 15 minutes
        run_strategy1_for_all_tickers(scenario='A')
        schedule.every(15).minutes.at("00:00").do(run_strategy1_for_all_tickers, scenario='A')
    elif scenario == 'B':
        # Schedule for Scenario B at specific times
        times = ["14:45", "29:45", "44:45", "59:45"]
        for t in times:
            schedule.every().hour.at(t).do(run_strategy1_for_all_tickers, scenario='B')
    else:
        logger.error(f"Invalid scenario '{scenario}' specified for Strategy 1.")

    while True:
        schedule.run_pending()
        time.sleep(1)

def run_strategy1_for_all_tickers(scenario=SCENARIO):
    tickers = TICKERS  # Define your tickers list in config
    for ticker in tickers:
        fetch_and_execute_strategy1(ticker, scenario)

# Start the scheduler in a separate thread if necessary
if __name__ == "__main__":
    threading.Thread(target=schedule_strategy1).start()