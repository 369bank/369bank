# indicators.py

import pandas as pd
import ta
import logging
from datetime import datetime, timezone

# Configure logging for indicators
logger = logging.getLogger('indicators')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(handler)

def calculate_indicators(df):
    """
    Calculate technical indicators and add them to the DataFrame.

    :param df: DataFrame with candle data
    :return: DataFrame with indicators
    """
    try:
        # Ensure necessary columns are present
        required_columns = ['timestamp', 'open', 'high', 'low', 'close']
        if not all(col in df.columns for col in required_columns):
            logger.error("DataFrame does not contain all required columns.")
            return df

        # Ensure columns are of float type
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        
        # Parabolic SAR
        psar_indicator = ta.trend.PSARIndicator(
            high=df['high'], low=df['low'], close=df['close'], step=0.02, max_step=0.2
        )
        # Calculating PSAR
        df['PSAR'] = psar_indicator.psar()
      
        # Determine PSAR trend direction
        df['PSAR_trend'] = 0
        df.loc[df['PSAR'] < df['close'], 'PSAR_trend'] = 1  # Bullish trend
        df.loc[df['PSAR'] > df['close'], 'PSAR_trend'] = -1  # Bearish trend

        # After determining PSAR trend
        logger.debug(f"PSAR trend values:\n{df[['timestamp', 'PSAR_trend']].tail()}")

        # Stochastic Oscillators with multiple window settings
        # STOCH_K_9_3
        stoch_9 = ta.momentum.StochasticOscillator(
            high=df['high'],
            low=df['low'],
            close=df['close'],
            window=9,
            smooth_window=3
        )
        df['STOCH_K_9_3'] = stoch_9.stoch()

        # STOCH_K_14_3
        stoch_14 = ta.momentum.StochasticOscillator(
            high=df['high'],
            low=df['low'],
            close=df['close'],
            window=14,
            smooth_window=3
        )
        df['STOCH_K_14_3'] = stoch_14.stoch()

        # STOCH_K_40_4
        stoch_40 = ta.momentum.StochasticOscillator(
            high=df['high'],
            low=df['low'],
            close=df['close'],
            window=40,
            smooth_window=4
        )
        df['STOCH_K_40_4'] = stoch_40.stoch()

        # STOCH_K_60_10_1
        stoch_60 = ta.momentum.StochasticOscillator(
            high=df['high'],
            low=df['low'],
            close=df['close'],
            window=60,
            smooth_window=10
        )
        df['STOCH_K_60_10_1'] = stoch_60.stoch()

        # Bollinger Bands
        bb = ta.volatility.BollingerBands(
            close=df['close'], window=20, window_dev=2
        )
        df['BB_percent_b'] = bb.bollinger_pband()
        # Handle NaN values using backfill and infer data types
        df.bfill(inplace=True)
        df.infer_objects()

        # Drop any remaining NaN values
        initial_length = len(df)
        df.dropna(inplace=True)
        final_length = len(df)
        dropped_rows = initial_length - final_length
        if dropped_rows > 0:
            logger.info(f"Dropped {dropped_rows} rows with NaN values after indicator calculations.")

        return df

    except Exception as e:
        logger.error(f"Error calculating indicators: {e}")
        return df