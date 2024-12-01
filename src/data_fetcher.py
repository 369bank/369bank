# data_fetcher.py

import sys
import os
import logging
import pandas as pd
import requests
from datetime import datetime, timedelta
import time
import threading
from functools import wraps
import schedule
import sqlite3

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import SANDBOX_MODE, TICKERS, DB_FILE, SCENARIO

# Configure logging
logger = logging.getLogger('data_fetcher')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(handler)

# Rate Limiter Decorator
def rate_limited(max_calls_per_second):
    min_interval = 1.0 / float(max_calls_per_second)
    lock = threading.Lock()
    last_time_called = [0.0]

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with lock:
                elapsed = time.perf_counter() - last_time_called[0]
                left_to_wait = min_interval - elapsed
                if left_to_wait > 0:
                    time.sleep(left_to_wait)
                last_time_called[0] = time.perf_counter()
                return func(*args, **kwargs)
        return wrapper
    return decorator

# Retry Decorator with Exponential Backoff
def retry_on_rate_limit(max_retries=5, initial_backoff=1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            backoff = initial_backoff
            while True:
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 429:
                        retries += 1
                        if retries > max_retries:
                            raise Exception("Maximum retries exceeded due to rate limiting.")
                        logger.warning(f"Rate limit exceeded. Retrying in {backoff} seconds...")
                        time.sleep(backoff)
                        backoff *= 2  # Exponential backoff
                    else:
                        raise
                except Exception as e:
                    raise
        return wrapper
    return decorator

@rate_limited(max_calls_per_second=100)
@retry_on_rate_limit(max_retries=5, initial_backoff=1)
def fetch_historical_data(ticker, start_time=None, end_time=None, granularity=60, limit=300):
    try:
        base_url = 'https://api.exchange.coinbase.com' if not SANDBOX_MODE else 'https://api-public.sandbox.exchange.coinbase.com'

        end_time_dt = datetime.utcnow()
        start_time_dt = end_time_dt - timedelta(seconds=granularity * limit)

        start_iso = start_time_dt.isoformat() + 'Z'
        end_iso = end_time_dt.isoformat() + 'Z'

        params = {
            'start': start_iso,
            'end': end_iso,
            'granularity': granularity
        }

        url = f'{base_url}/products/{ticker}/candles'
        response = requests.get(url, params=params)
        response.raise_for_status()

        candles = response.json()

        if not candles:
            logger.warning(f"No historical data fetched for {ticker}.")
            return None

        df = pd.DataFrame(candles, columns=['time', 'low', 'high', 'open', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['time'], unit='s', utc=True)
        df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
        df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
        df.sort_values('timestamp', inplace=True)

        return df
    except Exception as e:
        logger.error(f"An error occurred while fetching historical data for {ticker}: {e}")
        return None

def insert_data_into_db(ticker, df):
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS candlesticks_{ticker.replace('-', '_')} (
                timestamp TEXT PRIMARY KEY,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL
            )
        """)

        for _, row in df.iterrows():
            cursor.execute(f"""
                INSERT OR REPLACE INTO candlesticks_{ticker.replace('-', '_')} (timestamp, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                row['timestamp'].isoformat(),
                row['open'],
                row['high'],
                row['low'],
                row['close'],
                row['volume']
            ))

        conn.commit()
        conn.close()
        logger.info(f"Inserted data for {ticker} into the database.")
    except Exception as e:
        logger.error(f"Failed to insert data into database for {ticker}: {e}")

def fetch_and_store(ticker):
    try:
        granularity = 900  # 15 minutes
        limit = 100  # Number of data points

        data = fetch_historical_data(ticker, granularity=granularity, limit=limit)

        if data is not None:
            insert_data_into_db(ticker, data)
            logger.info(f"Fetched and inserted data for {ticker}.")
        else:
            logger.warning(f"No data fetched for {ticker}.")

    except Exception as e:
        logger.error(f"Error in fetch_and_store for {ticker}: {e}")

def run_scheduler():
    schedule.every(15).minutes.at(":00").do(lambda: [fetch_and_store(ticker) for ticker in TICKERS])

    while True:
        schedule.run_pending()
        time.sleep(1)

def start_data_fetcher(scenario=SCENARIO):
    if scenario == 'A':
        logger.info("Executing Scenario A: Immediate Data Capture")
        for ticker in TICKERS:
            fetch_and_store(ticker)
    elif scenario == 'B':
        logger.info("Executing Scenario B: Data Capture at Next Hour")
        utc_now = datetime.utcnow()
        next_hour = (utc_now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        wait_seconds = (next_hour - utc_now).total_seconds()
        logger.info(f"Waiting for {wait_seconds} seconds until the next hour starts.")
        time.sleep(wait_seconds)
        for ticker in TICKERS:
            fetch_and_store(ticker)
    else:
        raise ValueError("Invalid scenario. Use 'A' for immediate start or 'B' for next hour start.")

    scheduler_thread = threading.Thread(target=run_scheduler)
    scheduler_thread.daemon = True
    scheduler_thread.start()