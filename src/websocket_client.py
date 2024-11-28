# websocket_client.py

import websocket
import json
import logging
import threading
from config.config import TICKERS, SANDBOX_MODE
from process_websocket_data import process_websocket_data   

# Configure logging
logger = logging.getLogger('websocket_client')
logger.setLevel(logging.WARNING)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(handler)

def on_message(ws, message):
    data = json.loads(message)
    logger.info(f"Received message: {data}")
    # Process the message and integrate with your trading strategies
    process_websocket_data(data)

def on_error(ws, error):
    logger.error(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    logger.info("WebSocket connection closed.")

def on_open(ws):
    logger.info("WebSocket connection opened.")
    subscribe_message = {
        "type": "subscribe",
        "channels": [{
            "name": "ticker",
            "product_ids": TICKERS  # Use your list of valid tickers
        }]
    }
    ws.send(json.dumps(subscribe_message))

def run_websocket():
    if SANDBOX_MODE:
        websocket_url = 'wss://ws-feed-public.sandbox.exchange.coinbase.com'
    else:
        websocket_url = 'wss://ws-feed.exchange.coinbase.com'

    ws = websocket.WebSocketApp(
        websocket_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()