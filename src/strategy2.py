# strategy2.py

import logging
import sqlite3
import threading
import time
from datetime import datetime, timedelta
import pandas as pd
import os
import sys

# Add the parent directory to sys.path to locate the config package
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import necessary modules and configurations
from config.config import DB_FILE, TICKERS
from data_fetcher import fetch_historical_data

# Configure logging
logger = logging.getLogger('strategy2')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(handler)

def create_strategy2_table(conn):
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS strategy2_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            timestamp TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            "open_5m" REAL,
            "high_5m" REAL,
            "low_5m" REAL,
            "open_10m" REAL,
            "high_10m" REAL,
            "low_10m" REAL,
            "open_15m" REAL,
            "high_15m" REAL,
            "low_15m" REAL,
            "open_20m" REAL,
            "high_20m" REAL,
            "low_20m" REAL,
            "open_25m" REAL,
            "high_25m" REAL,
            "low_25m" REAL,
            "open_30m" REAL,
            "high_30m" REAL,
            "low_30m" REAL,
            "open_40m" REAL,
            "high_40m" REAL,
            "low_40m" REAL,
            "open_50m" REAL,
            "high_50m" REAL,
            "low_50m" REAL
        )
    ''')
    conn.commit()

def execute_strategy(ticker):
    try:
        steps = [
            (1, 0),    # Step number, wait time in seconds
            (2, 300),  # 5 minutes
            (3, 300),  # 5 minutes
            (4, 300),  # 5 minutes
            (5, 300),  # 5 minutes
            (6, 300),  # 5 minutes
            (7, 300),  # 5 minutes
            (8, 600),  # 10 minutes
            (9, 600),  # 10 minutes
        ]

        # Establish database connection
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        # Create the table if it doesn't exist
        create_strategy2_table(conn)

        # Initialize variables
        row_id = None

        for step_num, wait_time in steps:
            # Wait for the specified time
            if wait_time > 0:
                logger.info(f"{ticker}: Waiting for {wait_time} seconds before step {step_num}.")
                time.sleep(wait_time)

            # Fetch data
            data = fetch_historical_data(ticker, limit=1)
            if data is None or data.empty:
                logger.warning(f"No data fetched for {ticker} at step {step_num}.")
                continue

            # Get the latest data point
            latest_data = data.iloc[-1]

            if step_num == 1:
                # At step 1, insert a new record
                data_dict = {
                    'ticker': ticker,
                    'timestamp': latest_data['timestamp'].isoformat(),
                    'open': latest_data['open'],
                    'high': latest_data['high'],
                    'low': latest_data['low'],
                    'close': latest_data['close']
                }
                # Insert into database
                columns = ', '.join(data_dict.keys())
                placeholders = ', '.join(['?'] * len(data_dict))
                sql = f'INSERT INTO strategy2_data ({columns}) VALUES ({placeholders})'
                cursor.execute(sql, list(data_dict.values()))
                conn.commit()
                # Get the id of the inserted row
                row_id = cursor.lastrowid
                logger.debug(f"Inserted initial data for {ticker} with ID {row_id}.")
            else:
                # For subsequent steps, update the existing record
                total_seconds = sum([steps[i][1] for i in range(1, step_num)])
                total_minutes = total_seconds // 60
                column_prefix = f'{total_minutes}m'

                update_dict = {
                    f'{column_prefix}_open': latest_data['open'],
                    f'{column_prefix}_high': latest_data['high'],
                    f'{column_prefix}_low': latest_data['low']
                }

                # Update the database record
                set_clause = ', '.join([f'"{col}"=?' for col in update_dict.keys()])
                sql = f'UPDATE strategy2_data SET {set_clause} WHERE id=?'
                cursor.execute(sql, list(update_dict.values()) + [row_id])
                conn.commit()
                logger.debug(f"Updated data for {ticker} at step {step_num} (ID {row_id}).")

            logger.info(f"Executed Strategy 2 for {ticker} at step {step_num}.")

        # Close the database connection
        conn.close()

    except Exception as e:
        logger.error(f"An error occurred while executing Strategy 2 for {ticker}: {e}")

def run_strategy2():
    threads = []
    for ticker in TICKERS:
        thread = threading.Thread(target=execute_strategy, args=(ticker,))
        thread.start()
        threads.append(thread)

    # Optionally, wait for all threads to complete
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    run_strategy2()