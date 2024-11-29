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

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import SANDBOX_MODE, TICKERS

# Configure logging
logger = logging.getLogger('data_fetcher')
logger.setLevel(logging.WARNING)
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

# Combined Decorators Applied to the Fetch Function
@rate_limited(max_calls_per_second=100)
@retry_on_rate_limit(max_retries=5, initial_backoff=1)
def fetch_historical_data(ticker, start_time=None, end_time=None, granularity=60, limit=300):
    """
    Fetch historical candle data for a given ticker using Coinbase API.
    """
    try:
        if SANDBOX_MODE:
            base_url = 'https://api-public.sandbox.exchange.coinbase.com'
        else:
            base_url = 'https://api.exchange.coinbase.com'

        # Convert start_time and end_time to datetime objects
        if end_time is None:
            end_time_dt = datetime.utcnow()
        else:
            end_time_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))

        if start_time is None:
            delta = timedelta(seconds=granularity * limit)
            start_time_dt = end_time_dt - delta
        else:
            start_time_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))

        # Convert to ISO format
        start_iso = start_time_dt.isoformat()
        end_iso = end_time_dt.isoformat()

        # Prepare parameters
        params = {
            'start': start_iso,
            'end': end_iso,
            'granularity': granularity
        }

        url = f'{base_url}/products/{ticker}/candles'

        response = requests.get(url, params=params)

        # Raise an HTTPError if the response was unsuccessful
        response.raise_for_status()

        candles = response.json()

        if not candles:
            logger.warning(f"No historical data fetched for {ticker}.")
            return None

        # Transform the response data into a DataFrame
        df = pd.DataFrame(candles, columns=['time', 'low', 'high', 'open', 'close', 'volume'])

        # Convert 'time' to datetime with UTC
        df['timestamp'] = pd.to_datetime(df['time'], unit='s', utc=True)

        # Reorder columns
        df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]

        # Ensure numeric types
        df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)

        df.sort_values('timestamp', inplace=True)

        return df
    except Exception as e:
        logger.error(f"An error occurred while fetching historical data for {ticker}: {e}")
        return None

# Updated Scheduling Logic
def schedule_data_fetches(product_ids, granularity=60, limit=300, scenario='A'):
    """
    Schedules data fetches for a list of product IDs.
    """
    if scenario == 'A':
        # Scenario A: Data capture starts immediately
        start_time_dt = datetime.utcnow() - timedelta(seconds=granularity * limit)
        end_time_dt = datetime.utcnow()
    elif scenario == 'B':
        # Scenario B: Data capture starts at the next hour
        next_hour = (datetime.utcnow() + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        wait_seconds = (next_hour - datetime.utcnow()).total_seconds()
        logger.info(f"Waiting for {wait_seconds} seconds until the next hour starts.")
        time.sleep(wait_seconds)
        start_time_dt = next_hour - timedelta(seconds=granularity * limit)
        end_time_dt = next_hour
    else:
        raise ValueError("Invalid scenario. Use 'A' for immediate start or 'B' for next hour start.")

    # Convert to ISO format
    start_time = start_time_dt.isoformat() + 'Z'
    end_time = end_time_dt.isoformat() + 'Z'

    for product_id in product_ids:
        try:
            data = fetch_historical_data(
                ticker=product_id,
                start_time=start_time,
                end_time=end_time,
                granularity=granularity,
                limit=limit
            )
            if data is not None:
                # Process the fetched data as needed
                print(f"Fetched data for {product_id}")
                # For example, you might save the data to a file or database
                # data.to_csv(f"data/{product_id}_{granularity}.csv", index=False)
            else:
                print(f"No data fetched for {product_id}")
        except Exception as e:
            print(f"Error fetching data for {product_id}: {e}")
        # Optional: Add a small delay between products to spread out requests
        time.sleep(0.01)

if __name__ == "__main__":
    # Use the tickers from config.py
    product_ids = TICKERS

    # Set granularity and limit based on your indicators' requirements
    granularity = 60  # in seconds
    limit = 300  # Number of data points

    # Choose scenario 'A' or 'B'
    # Scenario A: Data capture starts immediately upon running the program.
    # Scenario B: Data capture starts at the commencement of the next hour.

    scenario = 'B'  # Change to 'B' for Scenario B

    schedule_data_fetches(product_ids, granularity, limit, scenario)