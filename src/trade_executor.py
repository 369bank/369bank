# trade_executor.py

import time
import threading
import logging
from datetime import datetime
from auth import get_client
from db_manager import log_trade_open, log_trade_close
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

def place_order(ticker, side, amount, strategy):
    """
    Place a limit (maker) order and handle order monitoring and cancellation.

    :param ticker: str, the trading pair (e.g., 'BTC-USD')
    :param side: str, 'BUY' or 'SELL'
    :param amount: float, the amount to buy or sell
    :param strategy: str, the strategy name initiating the order
    """
    try:
        # Fetch order book to determine the best price
        order_book = client.get_market_order_book(ticker, level='1')
        if side.upper() == 'BUY':
            # For buying, set price slightly lower than the best ask
            best_ask = float(order_book['asks'][0]['price'])
            price = round(best_ask * 0.9999, 2)  # Adjust the multiplier as needed
        else:
            # For selling, set price slightly higher than the best bid
            best_bid = float(order_book['bids'][0]['price'])
            price = round(best_bid * 1.0001, 2)  # Adjust the multiplier as needed

        # Create the order payload
        order = {
            'market_pair': ticker,
            'order_side': side.lower(),
            'order_type': 'limit',
            'size': str(amount),
            'price': str(price),
            'post_only': True  # Ensure the order is a maker order
        }

        # Place the limit order
        response = client.place_order(**order)
        logger.info(f"Placed limit {side} order for {ticker}: {response}")

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

        # Start a thread to monitor the order
        monitoring_thread = threading.Thread(
            target=monitor_order,
            args=(order_id, trade_id, ticker, side, strategy, 280)
        )
        monitoring_thread.start()

    except Exception as e:
        logger.error(f"Error placing limit {side} order for {ticker}: {e}")

def monitor_order(order_id, trade_id, ticker, side, strategy, timeout):
    """
    Monitor the order for a given timeout period and cancel if not filled.

    :param order_id: str, the ID of the placed order
    :param trade_id: int, the database ID of the trade
    :param ticker: str, the trading pair
    :param side: str, 'BUY' or 'SELL'
    :param strategy: str, the strategy name
    :param timeout: int, time in seconds to wait before cancelling
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            order_status = client.get_order(order_id)
            if order_status['status'] == 'FILLED':
                logger.info(f"Order {order_id} filled.")
                # For SELL orders, calculate profit/loss
                if side.upper() == 'SELL':
                    sell_price = float(order_status['avg_price'])
                    sell_amount = float(order_status['filled_size'])
                    buy_trade = get_latest_buy_trade(ticker, strategy)
                    if buy_trade:
                        profit_loss = (sell_price - buy_trade['price']) * sell_amount
                    else:
                        profit_loss = 0
                    log_trade_close(
                        trade_id=trade_id,
                        sell_price=sell_price,
                        sell_amount=sell_amount,
                        profit_loss=profit_loss
                    )
                return
            time.sleep(5)  # Wait before checking again
        except Exception as e:
            logger.error(f"Error monitoring order {order_id}: {e}")
            time.sleep(5)

    # Cancel the order if not filled
    try:
        client.cancel_order(order_id)
        logger.info(f"Order {order_id} cancelled after timeout.")
    except Exception as e:
        logger.error(f"Error cancelling order {order_id}: {e}")

def monitor_order_with_market_fallback(order_id, trade_id, ticker, side, strategy, timeout):
    """
    Monitor the order and place a market order if not filled after timeout.

    :param order_id: str, the ID of the placed order
    :param trade_id: int, the database ID of the trade
    :param ticker: str, the trading pair
    :param side: str, 'BUY' or 'SELL'
    :param strategy: str, the strategy name
    :param timeout: int, time in seconds to wait before placing market order
    """
    monitor_order(order_id, trade_id, ticker, side, strategy, timeout)

    # After timeout, check if order is filled
    order_status = client.get_order(order_id)
    if order_status['status'] != 'done':
        # Cancel the unfilled order
        try:
            client.cancel_order(order_id)
            logger.info(f"Order {order_id} cancelled after timeout.")
        except Exception as e:
            logger.error(f"Error cancelling order {order_id}: {e}")

        # Place a market order to sell the remaining amount
        remaining_size = float(order_status['size']) - float(order_status['filled_size'])
        if remaining_size > 0:
            market_order = {
                'type': 'market',
                'side': side.lower(),
                'product_id': ticker,
                'size': str(remaining_size)
            }
            try:
                market_response = client.place_order(**market_order)
                logger.info(f"Placed market {side} order for {ticker}: {market_response}")

                # Record the trade closure in the database
                executed_value = float(market_response.get('executed_value', 0))
                sell_price = executed_value / remaining_size if remaining_size else 0
                sell_amount = remaining_size
                buy_trade = get_latest_buy_trade(ticker, strategy)
                if buy_trade:
                    profit_loss = (sell_price - buy_trade['buy_price']) * sell_amount
                else:
                    profit_loss = 0
                log_trade_close(
                    trade_id=trade_id,
                    sell_price=sell_price,
                    sell_amount=sell_amount,
                    profit_loss=profit_loss
                )
            except Exception as e:
                logger.error(f"Error placing market {side} order for {ticker}: {e}")
    else:
        logger.info(f"Order {order_id} was filled before timeout.")

def schedule_sell_order(ticker, sell_time, strategy):
    """
    Schedule a sell order for a specific time in the future.

    :param ticker: str, the trading pair
    :param sell_time: datetime, the time to execute the sell order
    :param strategy: str, the strategy name
    """
    delay = (sell_time - datetime.utcnow()).total_seconds()
    if delay < 0:
        logger.error(f"Sell time {sell_time} is in the past. Cannot schedule sell order.")
        return
    threading.Timer(delay, execute_sell_order, args=(ticker, strategy)).start()

def execute_sell_order(ticker, strategy):
    """
    Execute a sell order according to the strategy.

    :param ticker: str, the trading pair
    :param strategy: str, the strategy name
    """
    # Determine the amount to sell
    base_currency = ticker.split('-')[0]
    amount = get_holdings(base_currency)
    if amount is None or amount == 0:
        logger.warning(f"No holdings to sell for {ticker}.")
        return

    # Place the sell order
    place_order(ticker, side='SELL', amount=amount, strategy=strategy)

def get_holdings(currency):
    """
    Retrieve the available balance for a specific currency.

    :param currency: str, the currency symbol (e.g., 'BTC')
    :return: float, the available balance
    """
    try:
        accounts = client.list_accounts()
        for account in accounts:
            if account['currency'] == currency:
                return float(account['available_balance']['value'])
    except Exception as e:
        logger.error(f"Error fetching holdings for {currency}: {e}")
    return None