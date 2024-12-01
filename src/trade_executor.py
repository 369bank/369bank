# trade_executor.py

import time
import threading
import logging
from datetime import datetime
from auth import get_client
from db_manager import log_trade_open, log_trade_close, get_latest_buy_trade
from utils import get_current_price  # Assuming utils.py contains this function

# Configure logging
logger = logging.getLogger('trade_executor')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(handler)

# Initialize the authenticated client
client = get_client()

def place_order(ticker, side, amount, strategy, order_type='limit', time_in_force='GTC', make_order=True):
    """
    Place an order and return the order ID.

    :param ticker: str, the trading pair (e.g., 'BTC-USD')
    :param side: str, 'BUY' or 'SELL'
    :param amount: float, the amount to buy or sell
    :param strategy: str, the strategy name initiating the order
    :param order_type: str, 'limit' or 'market'
    :param time_in_force: str, e.g., 'GTC' (Good Till Canceled)
    :param make_order: bool, whether to place a maker order (limit order)
    :return: str, the order ID
    """
    try:
        # Fetch order book to determine the best price
        order_book = client.get_product_order_book(ticker, level=1)
        if side.upper() == 'BUY':
            # For buying, set price slightly lower than the best ask
            best_ask = float(order_book['asks'][0][0])
            price = round(best_ask * 0.9999, 8)  # Adjust precision as needed
        else:
            # For selling, set price slightly higher than the best bid
            best_bid = float(order_book['bids'][0][0])
            price = round(best_bid * 1.0001, 8)  # Adjust precision as needed

        # Create the order payload
        order = {
            'product_id': ticker,
            'side': side.lower(),
            'order_type': order_type,
            'size': str(amount),
        }

        if order_type == 'limit':
            order['price'] = str(price)
            order['post_only'] = make_order
            order['time_in_force'] = time_in_force

        # Place the order
        response = client.place_order(**order)
        logger.info(f"Placed {order_type} {side} order for {ticker}: {response}")

        # Record the trade initiation in the database
        order_id = response['order_id']
        trade_id = log_trade_open(
            ticker=ticker,
            strategy=strategy,
            side=side.upper(),
            price=price,
            amount=amount,
            order_id=order_id
        )

        return order_id

    except Exception as e:
        logger.error(f"Error placing {order_type} {side} order for {ticker}: {e}")
        return None

def is_order_filled(order_id):
    """
    Check if the order is fully filled.

    :param order_id: str, the ID of the order
    :return: bool, True if filled, False otherwise
    """
    try:
        order_status = client.get_order(order_id)
        status = order_status['status']
        filled_size = float(order_status.get('filled_size', 0))
        size = float(order_status.get('size', 0))
        if status == 'done' and filled_size >= size:
            return True
        else:
            return False
    except Exception as e:
        logger.error(f"Error checking order status for {order_id}: {e}")
        return False

def cancel_order(order_id):
    """
    Cancel the order.

    :param order_id: str, the ID of the order
    :return: bool, True if canceled successfully, False otherwise
    """
    try:
        result = client.cancel_order(order_id)
        logger.info(f"Order {order_id} cancelled: {result}")
        return True
    except Exception as e:
        logger.error(f"Error cancelling order {order_id}: {e}")
        return False

def close_position(ticker, amount, strategy, order_type='limit', time_in_force='GTC', make_order=True):
    """
    Close the position by placing a sell order.

    :param ticker: str, the trading pair
    :param amount: float, the amount to sell
    :param strategy: str, the strategy name
    :param order_type: str, 'limit' or 'market'
    :param time_in_force: str, e.g., 'GTC'
    :param make_order: bool, whether to place a maker order (limit order)
    :return: str, the order ID
    """
    return place_order(ticker, 'SELL', amount, strategy, order_type, time_in_force, make_order)

def get_holdings(currency):
    """
    Retrieve the available balance for a specific currency.

    :param currency: str, the currency symbol (e.g., 'BTC')
    :return: float, the available balance
    """
    try:
        accounts = client.get_accounts()
        for account in accounts:
            if account['currency'] == currency:
                return float(account['balance'])
    except Exception as e:
        logger.error(f"Error fetching holdings for {currency}: {e}")
    return 0.0