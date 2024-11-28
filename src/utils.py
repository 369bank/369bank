# utils.py

import logging
from auth import get_client

# Configure logging
logger = logging.getLogger('utils')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(handler)

# Initialize the authenticated client
client = get_client()

def get_current_price(ticker):
    """
    Fetch the current market price for the given ticker.

    :param ticker: str, the trading pair
    :return: float, the current price
    """
    try:
        ticker_data = client.get_product_ticker(product_id=ticker)
        return float(ticker_data['price'])
    except Exception as e:
        logger.error(f"Error fetching price for {ticker}: {e}")
        return None