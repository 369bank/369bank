# main.py 

import sys
import os
import threading
import time
import logging

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import TICKERS
from data_fetcher import schedule_data_fetches
from indicators import calculate_indicators
from strategy1 import strategy1
from strategy2 import run_strategy2
from db_manager import initialize_db, log_error
from process_websocket_data import initialize_historical_data, historical_data
from logging.handlers import RotatingFileHandler
from websocket_client import run_websocket
from datetime import datetime, timedelta, timezone

# Configure logging
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOGS_DIR = os.path.join(BASE_DIR, 'logs')
if not os.path.exists(LOGS_DIR):
    os.makedirs(LOGS_DIR)

log_file = os.path.join(LOGS_DIR, 'main.log')
logger = logging.getLogger('main')
logger.setLevel(logging.WARNING)
handler = RotatingFileHandler(
    log_file, maxBytes=5 * 1024 * 1024, backupCount=5
)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(handler)

def main():
    # Initialize the database
    initialize_db()
    logger.info("Database initialized.")

    # Initialize historical data
    initialize_historical_data()
    logger.info("Historical data initialized.")

    # Start the WebSocket client in a separate thread
    ws_thread = threading.Thread(target=run_websocket, daemon=True)
    ws_thread.start()
    logger.info("WebSocket client started.")

    # Start Strategy 2 in a separate daemon thread
    strategy2_thread = threading.Thread(target=run_strategy2, daemon=True)
    strategy2_thread.start()
    logger.info("Strategy 2 started.")

    try:
        # Run Strategy 1 in the main thread
        while True:
            for ticker in TICKERS:
                try:
                    # Use the historical data from process_websocket_data
                    if ticker not in historical_data or historical_data[ticker].empty:
                        logger.warning(f"No historical data for {ticker}. Skipping Strategy 1.")
                        continue

                    df = historical_data[ticker].copy()

                    # Calculate indicators
                    df = calculate_indicators(df)

                    # Apply Strategy 1
                    strategy1(df, ticker)
                except Exception as e:
                    logger.error(f"Error in Strategy 1 for {ticker}: {e}")
                    log_error(str(e))

            # Sleep until the next 15-minute interval
            now = datetime.now(timezone.utc)
            next_run = now + timedelta(
                minutes=15 - now.minute % 15,
                seconds=-now.second,
                microseconds=-now.microsecond
            )
            sleep_seconds = (next_run - now).total_seconds()
            logger.info(f"Sleeping for {sleep_seconds} seconds until next run.")
            time.sleep(sleep_seconds)
    except KeyboardInterrupt:
        logger.info("Trading bot stopped by user.")
    finally:
        # Clean up threads if necessary
        logger.info("Shutting down...")

if __name__ == "__main__":

    # You can set granularity and limit here if needed
    granularity = 60  # in seconds
    limit = 300  # Number of data points

    # Schedule data fetches
    schedule_data_fetches(TICKERS, granularity, limit)

    main()