# strategy2.py

import logging
import sqlite3
import threading
import time
from datetime import datetime, timedelta
import pandas as pd
import os
import sys
import random
import traceback

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import DB_FILE, TICKERS, SCENARIO, BUY_AMOUNTS, RISK_MANAGEMENT_PARAMS
from data_fetcher import fetch_historical_data
from trade_executor import place_order, close_position, is_order_filled, cancel_order
from risk_manager import check_risk_management
from db_manager import log_error

# Configure logging
logger = logging.getLogger('strategy2')
logger.setLevel(logging.INFO)
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
            "close_5m" REAL,
            "open_10m" REAL,
            "high_10m" REAL,
            "low_10m" REAL,
            "close_10m" REAL,
            "open_15m" REAL,
            "high_15m" REAL,
            "low_15m" REAL,
            "close_15m" REAL,
            "open_20m" REAL,
            "high_20m" REAL,
            "low_20m" REAL,
            "close_20m" REAL,
            "open_25m" REAL,
            "high_25m" REAL,
            "low_25m" REAL,
            "close_25m" REAL,
            "open_30m" REAL,
            "high_30m" REAL,
            "low_30m" REAL,
            "close_30m" REAL,
            "open_40m" REAL,
            "high_40m" REAL,
            "low_40m" REAL,
            "close_40m" REAL,
            "open_50m" REAL,
            "high_50m" REAL,
            "low_50m" REAL,
            "close_50m" REAL
        )
    ''')
    conn.commit()

def fetch_data_with_retry(ticker, granularity, max_retries=3, initial_delay=1, backoff_factor=2):
    delay = initial_delay
    for attempt in range(1, max_retries + 1):
        try:
            data = fetch_historical_data(ticker, granularity=granularity, limit=1)
            if data is not None and not data.empty:
                return data.iloc[-1]
            else:
                raise ValueError("No data returned")
        except Exception as e:
            logger.error(f"{ticker}: Attempt {attempt} failed with error: {e}")
            logger.debug(f"Stack Trace: {traceback.format_exc()}")
            if 'rate limit' in str(e).lower() or '429' in str(e):
                wait_time = 60  # Adjust based on API's rate limit reset time
                logger.warning(f"{ticker}: Rate limit exceeded. Waiting {wait_time} seconds before retrying.")
                time.sleep(wait_time)
            else:
                if attempt < max_retries:
                    sleep_time = delay + random.uniform(0, 1)
                    logger.info(f"{ticker}: Retrying in {sleep_time:.2f} seconds...")
                    time.sleep(sleep_time)
                    delay *= backoff_factor
                else:
                    logger.error(f"{ticker}: Failed to fetch data after {max_retries} attempts.")
                    return None

def execute_strategy(ticker, scenario=SCENARIO):
    while True:
        try:
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()
            create_strategy2_table(conn)

            # Reset the flag at the start of each hour
            order_placed_this_hour = False

            # Scenario Logic
            if scenario == 'A':
                logger.info(f"{ticker}: Starting data capture immediately (Scenario A).")
            elif scenario == 'B':
                # Synchronize to the start of the next hour
                now = datetime.utcnow()
                seconds_to_wait = ((60 - now.minute) % 60) * 60 - now.second - now.microsecond / 1e6
                logger.info(f"{ticker}: Waiting {seconds_to_wait} seconds until the start of the next hour (Scenario B).")
                time.sleep(seconds_to_wait)
                logger.info(f"{ticker}: Starting new hourly cycle.")
            else:
                logger.error(f"Invalid scenario '{scenario}' specified for {ticker}. Defaulting to Scenario B.")
                now = datetime.utcnow()
                seconds_to_wait = ((60 - now.minute) % 60) * 60 - now.second - now.microsecond / 1e6
                logger.info(f"{ticker}: Waiting {seconds_to_wait} seconds until the start of the next hour.")
                time.sleep(seconds_to_wait)
                logger.info(f"{ticker}: Starting new hourly cycle.")

            # Fetch the opening price of the 1-hour candle
            one_hour_data = fetch_data_with_retry(ticker, granularity=3600)
            if one_hour_data is None:
                logger.warning(f"{ticker}: Failed to fetch 1-hour data. Skipping to next cycle.")
                continue
            one_hour_open = one_hour_data['open']
            one_hour_timestamp = one_hour_data['timestamp']

            # Insert initial data into database
            data_dict = {
                'ticker': ticker,
                'timestamp': one_hour_timestamp.isoformat(),
                'open': one_hour_open,
                'high': one_hour_data['high'],
                'low': one_hour_data['low'],
                'close': one_hour_data['close']
            }
            columns = ', '.join(data_dict.keys())
            placeholders = ', '.join(['?'] * len(data_dict))
            sql = f'INSERT INTO strategy2_data ({columns}) VALUES ({placeholders})'
            cursor.execute(sql, list(data_dict.values()))
            conn.commit()
            row_id = cursor.lastrowid
            logger.debug(f"Inserted initial data for {ticker} with ID {row_id}.")

            # Initialize variables
            monitoring_steps = [
                {'wait': 300,  'threshold': 0.90, 'granularity': 300, 'step': '5m'},   # 5 minutes
                {'wait': 300,  'threshold': 0.95, 'granularity': 600, 'step': '10m'},  # 10 minutes
                {'wait': 300,  'threshold': 0.95, 'granularity': 900, 'step': '15m'},  # 15 minutes
                {'wait': 300,  'threshold': 0.95, 'granularity': 1200, 'step': '20m'}, # 20 minutes
                {'wait': 300,  'threshold': 0.95, 'granularity': 1500, 'step': '25m'}, # 25 minutes
                {'wait': 300,  'threshold': 0.95, 'granularity': 1800, 'step': '30m'}, # 30 minutes
                {'wait': 600,  'threshold': 0.95, 'granularity': 2400, 'step': '40m'}, # 40 minutes
                {'wait': 600,  'threshold': 0.94, 'granularity': 3000, 'step': '50m'}, # 50 minutes
            ]

            for step in monitoring_steps:
                if order_placed_this_hour:
                    break
                wait_time = step['wait']
                threshold = step['threshold']
                granularity = step['granularity']
                step_label = step['step']

                # Randomize wait_time slightly to avoid synchronization
                wait_time_with_jitter = wait_time + random.uniform(-5, 5)
                if wait_time_with_jitter > 0:
                    logger.info(f"{ticker}: Waiting for {wait_time_with_jitter:.2f} seconds before step {step_label}.")
                    time.sleep(wait_time_with_jitter)

                # Fetch the latest candle
                latest_data = fetch_data_with_retry(ticker, granularity=granularity)
                if latest_data is None:
                    logger.warning(f"{ticker}: No data fetched at step {step_label}. Skipping to next step.")
                    continue

                # Update the database with the latest data
                update_dict = {
                    f'open_{step_label}': latest_data['open'],
                    f'high_{step_label}': latest_data['high'],
                    f'low_{step_label}': latest_data['low'],
                    f'close_{step_label}': latest_data['close']
                }
                set_clause = ', '.join([f'"{col}"=?' for col in update_dict.keys()])
                sql = f'UPDATE strategy2_data SET {set_clause} WHERE id=?'
                cursor.execute(sql, list(update_dict.values()) + [row_id])
                conn.commit()
                logger.debug(f"Updated data for {ticker} at step {step_label} (ID {row_id}).")

                logger.info(f"Executed Strategy 2 for {ticker} at step {step_label}.")

                # Calculate the ratio
                close_price = latest_data['close']
                ratio = close_price / one_hour_open
                logger.info(f"{ticker}: Ratio at step {step_label} is {ratio:.4f}")

                # Check if risk management allows placing an order
                if not check_risk_management('Strategy 2'):
                    logger.warning(f"{ticker}: Risk management prevents placing a new order.")
                    break  # Stop monitoring for this hour

                # Check the condition
                if ratio < threshold:
                    logger.info(f"{ticker}: Condition met at step {step_label}.")
                    if order_placed_this_hour:
                        logger.info(f"{ticker}: Order already placed this hour. Skipping further orders.")
                        break

                    amount = BUY_AMOUNTS.get(ticker, 10)  # Default amount if not specified
                    order_id = place_order(
                        ticker,
                        side='BUY',
                        amount=amount,
                        strategy='Strategy 2',
                        order_type='limit',
                        time_in_force='GTC',
                        make_order=True  # Ensures it's a maker order
                    )
                    if order_id:
                        order_placed_this_hour = True
                        open_positions.setdefault(ticker, []).append({
                            'entry_time': datetime.utcnow(),
                            'order_id': order_id,
                            'amount': amount,
                            'entry_price': close_price
                        })
                        logger.info(f"{ticker}: Order placed with ID {order_id}.")
                        # Implement order management rules
                        manage_order(ticker, order_id)
                    else:
                        logger.error(f"{ticker}: Failed to place order.")
                    break  # Stop monitoring after placing an order

            # Update final high, low, close values
            cursor.execute(f"SELECT * FROM strategy2_data WHERE id=?", (row_id,))
            record = cursor.fetchone()
            if record:
                # Record indices based on the table schema
                # Index 0: id
                # Index 1: ticker
                # Index 2: timestamp
                # Index 3: open
                # Index 4: high
                # Index 5: low
                # Index 6: close
                high_values = [record[4]]  # initial high
                low_values = [record[5]]   # initial low
                # Collect high_* and low_* values
                for i in range(7, len(record), 4):
                    if record[i] is not None:  # open_* step
                        high_values.append(record[i+1])  # high_* step
                        low_values.append(record[i+2])   # low_* step
                final_high = max(high_values)
                final_low = min(low_values)
                final_close = latest_data['close']

                cursor.execute('''
                    UPDATE strategy2_data
                    SET high = ?, low = ?, close = ?
                    WHERE id = ?
                ''', (final_high, final_low, final_close, row_id))
                conn.commit()
                logger.info(f"{ticker}: Updated final high, low, close values.")

            # Close the database connection
            conn.close()

            # Positions are closed separately after 8 hours from entry
            # So we need to check if any positions need to be closed
            current_time = datetime.utcnow()
            positions_to_close = []
            for position in open_positions.get(ticker, []):
                close_time = position['entry_time'] + timedelta(hours=8)
                if current_time >= close_time:
                    positions_to_close.append(position)

            for position in positions_to_close:
                logger.info(f"{ticker}: Closing position after 8 hours.")
                amount = position['amount']
                close_order_id = close_position(
                    ticker,
                    amount=amount,
                    strategy='Strategy 2',
                    order_type='limit',
                    time_in_force='GTC',
                    make_order=True  # Ensures it's a maker order
                )
                if close_order_id:
                    logger.info(f"{ticker}: Position closed with order ID {close_order_id}.")
                    open_positions[ticker].remove(position)
                else:
                    logger.error(f"{ticker}: Failed to close position.")

            # **Add this block to wait until the start of the next hour**
            now = datetime.utcnow()
            seconds_to_wait = ((60 - now.minute) % 60) * 60 - now.second - now.microsecond / 1e6
            if seconds_to_wait > 0:
                logger.info(f"{ticker}: Waiting {seconds_to_wait} seconds until the start of the next hour.")
                time.sleep(seconds_to_wait)

        except Exception as e:
            logger.error(f"An error occurred while executing Strategy 2 for {ticker}: {e}")
            logger.debug(f"Stack Trace: {traceback.format_exc()}")
            time.sleep(60)  # Wait before retrying

def manage_order(ticker, order_id):
    """
    Manages the order according to the specified order management rules.
    """
    try:
        # Wait 2.8 minutes and check if the order is filled
        time.sleep(170)
        if not is_order_filled(order_id):
            logger.info(f"{ticker}: Order not fully filled after 3 minutes. Cancelling order.")
            cancel_order(order_id)
            # Proceed with any filled amount
        else:
            logger.info(f"{ticker}: Order fully filled.")
    except Exception as e:
        logger.error(f"{ticker}: Error in managing order: {e}")
        logger.debug(f"{ticker}: {traceback.format_exc()}")
        log_error(str(e))

def run_strategy2(scenario=SCENARIO):
    initial_delay = 0  # No initial delay before starting the first ticker
    interval_delay = 10  # 10-second delay between tickers
    threads = []
    for idx, ticker in enumerate(TICKERS):
        delay = initial_delay + idx * interval_delay
        logger.info(f"Scheduling {ticker} to start in {delay} seconds.")
        thread = threading.Thread(target=start_strategy2_with_delay, args=(ticker, delay, scenario))
        thread.daemon = True
        thread.start()
        threads.append(thread)

def start_strategy2_with_delay(ticker, delay, scenario):
    logger.info(f"{ticker}: Sleeping for {delay} seconds before starting.")
    time.sleep(delay)
    execute_strategy(ticker, scenario)

# Global dictionary to keep track of open positions
open_positions = {}

if __name__ == "__main__":
    run_strategy2()