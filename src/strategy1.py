# strategy1.py

import pandas as pd
import logging
import os
from datetime import datetime, timezone
from config.config import STRATEGY_1, BUY_AMOUNTS, CAPTURE_STRATEGY1_DATA
from trade_executor import place_order
from risk_manager import check_risk_management
from db_manager import log_strategy1_data

# Configure logging
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOGS_DIR = os.path.join(BASE_DIR, 'logs')
if not os.path.exists(LOGS_DIR):
    os.makedirs(LOGS_DIR)

from logging.handlers import RotatingFileHandler

log_file = os.path.join(LOGS_DIR, 'strategy1.log')
logger = logging.getLogger('strategy1')
logger.setLevel(logging.WARNING)
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

    # Verify that 'PSAR' and 'PSAR_trend' are in the DataFrame
    if 'PSAR' not in df.columns or 'PSAR_trend' not in df.columns:
        logger.error(f"PSAR indicators not found in DataFrame for {ticker}.")
        return

    # Check if required indicators are present
    required_indicators = [
        'PSAR_trend',  # Updated column name
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
    place_order(ticker, side='BUY', amount=amount, strategy='Strategy 1')