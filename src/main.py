# main.py

import sys
import os
import threading
import time
import logging
import sqlite3
import pandas as pd
import schedule
from datetime import datetime, timedelta, timezone

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import TICKERS, DB_FILE, SCENARIO
from data_fetcher import start_data_fetcher
from indicators import calculate_indicators
from strategy1 import schedule_strategy1
from strategy2 import run_strategy2
from db_manager import initialize_db, log_error
from logging.handlers import RotatingFileHandler
from notifier import send_email
from reporting import send_daily_report

# Configure logging
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOGS_DIR = os.path.join(BASE_DIR, 'logs')
if not os.path.exists(LOGS_DIR):
    os.makedirs(LOGS_DIR)

log_file = os.path.join(LOGS_DIR, 'main.log')
logger = logging.getLogger('main')
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(
    log_file, maxBytes=5 * 1024 * 1024, backupCount=5
)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(handler)

def clean_old_candlestick_data():
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        # Get current UTC time
        now = datetime.utcnow()

        # Calculate the cutoff time (24 hours ago)
        cutoff_time = now - timedelta(hours=24)
        cutoff_timestamp = cutoff_time.isoformat()

        # Get all table names starting with 'candlesticks_'
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'candlesticks_%';")
        tables = cursor.fetchall()

        for (table_name,) in tables:
            logger.info(f"Cleaning old data from {table_name}.")
            cursor.execute(f"DELETE FROM {table_name} WHERE timestamp < ?", (cutoff_timestamp,))
            conn.commit()
            logger.info(f"Old data deleted from {table_name}.")

        conn.close()
        logger.info("Cleaned old candlestick data from all tables.")
    except Exception as e:
        logger.error(f"Error while cleaning old candlestick data: {e}")

def run_scheduler():
    try:
        # Schedule the daily report at 7 AM UTC
        schedule.every().day.at("07:00").do(send_daily_report)
        # Schedule the cleanup function to run daily at 00:00 UTC
        schedule.every().day.at("00:00").do(clean_old_candlestick_data)

        while True:
            schedule.run_pending()
            time.sleep(60)  # Wait one minute between checks
    except Exception as e:
        logger.error(f"Scheduler encountered an error: {e}")

def main():
    initialize_db()
    logger.info("Database initialized.")

    # Start data fetching in a separate thread with Scenario A or B as needed
    scenario = SCENARIO  # Get scenario from config

    # Start Strategy 1 first
    strategy1_thread = threading.Thread(target=schedule_strategy1, args=(scenario,))
    strategy1_thread.daemon = True
    strategy1_thread.start()
    logger.info("Strategy 1 started.")

    # Wait to ensure Strategy 1 has started and completed initial data fetching
    time.sleep(20)  # Reduced sleep time to 20 seconds

    # Start data fetcher (for continuous data fetching if applicable)
    data_fetcher_thread = threading.Thread(target=start_data_fetcher, args=(scenario,), daemon=True)
    data_fetcher_thread.start()
    logger.info("Data fetcher started.")

    # Start Strategy 2
    strategy2_thread = threading.Thread(target=run_strategy2, args=(scenario,), daemon=True)
    strategy2_thread.start()
    logger.info("Strategy 2 started.")

    # Send initialization email
    subject = "Trading Bot Initialized"
    body = "The trading bot has started."
    send_email(subject, body)

    # Start the scheduler in a separate thread
    scheduler_thread = threading.Thread(target=run_scheduler)
    scheduler_thread.daemon = True
    scheduler_thread.start()

    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Trading bot stopped by user.")
    finally:
        logger.info("Shutting down...")

if __name__ == "__main__":
    main()