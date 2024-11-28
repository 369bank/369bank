# process_websocket_data.py

import pandas as pd
from datetime import datetime, timezone
from indicators import calculate_indicators
from data_fetcher import fetch_historical_data
import logging

# Configure logging
logger = logging.getLogger('websocket_data_processor')
logger.setLevel(logging.WARNING)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(handler)

# Initialize data structures for historical data
historical_data = {}

# List of tickers to fetch historical data for
from config.config import TICKERS

def initialize_historical_data():
    """
    Fetch historical data for each ticker and store it in the historical_data dictionary.
    """
    for ticker in TICKERS:
        try:
            # Fetch historical data with sufficient data points
            df = fetch_historical_data(
                ticker=ticker,
                granularity=900,  # 15 minutes in seconds
                limit=300  # Fetch at least 300 data points
            )

            if df is not None and not df.empty:
                # Ensure the DataFrame has the required columns
                required_columns = ['timestamp', 'open', 'high', 'low', 'close']
                if not all(col in df.columns for col in required_columns):
                    logger.error(f"Historical data for {ticker} does not contain all required columns.")
                    continue

                # Convert 'timestamp' to datetime with UTC
                df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)

                # Ensure numeric columns are of type float
                numeric_cols = ['open', 'high', 'low', 'close']
                df[numeric_cols] = df[numeric_cols].astype(float)

                # Store the DataFrame in the historical_data dictionary
                historical_data[ticker] = df
                logger.info(f"Historical data for {ticker} initialized.")
            else:
                logger.warning(f"No historical data fetched for {ticker}.")
        except Exception as e:
            logger.error(f"Error fetching historical data for {ticker}: {e}")

def process_websocket_data(data):
    try:
        # Check if the message type is 'ticker'
        if data['type'] != 'ticker':
            return

        product_id = data['product_id']
        price = float(data['price'])
        timestamp = data.get('time', datetime.now(timezone.utc).isoformat())

        # Initialize historical data if not already done
        if product_id not in historical_data:
            logger.warning(f"No historical data for {product_id}. Fetching now...")
            df = fetch_historical_data(
                ticker=product_id,
                granularity=900,  # 15 minutes in seconds
                limit=300  # Adjust as needed
            )
            if df is not None and not df.empty:
                historical_data[product_id] = df
            else:
                # Initialize empty DataFrame if unable to fetch historical data
                historical_data[product_id] = pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close'])

        # Create a new row with the current price for all OHLC columns

        new_row = pd.DataFrame({
            'timestamp': [pd.to_datetime(timestamp, utc=True)],
            'open': [price],
            'high': [price],
            'low': [price],
            'close': [price]
        })

        # Append or assign the new row to the historical data
        historical_data[product_id] = pd.concat([historical_data[product_id], new_row], ignore_index=True)

        # Limit DataFrame size to prevent memory issues
        historical_data[product_id] = historical_data[product_id].tail(500)

        # Get the DataFrame
        df = historical_data[product_id]

        # Ensure required columns are present
        required_columns = ['timestamp', 'open', 'high', 'low', 'close']
        if not all(col in df.columns for col in required_columns):
            logger.error(f"DataFrame for {product_id} does not contain all required columns.")
            return

        # Ensure data types are correct
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        numeric_cols = ['open', 'high', 'low', 'close']
        df[numeric_cols] = df[numeric_cols].astype(float)

        # Drop rows with NaN values
        df.dropna(inplace=True)

        # Update the historical data
        historical_data[product_id] = df

    except Exception as e:
        logger.error(f"Error processing data for {product_id}: {e}")