# db_manager.py

import sqlite3
import logging
import os
from datetime import datetime

# Get the absolute path to the logs directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOGS_DIR = os.path.join(BASE_DIR, 'logs')
if not os.path.exists(LOGS_DIR):
    os.makedirs(LOGS_DIR)

from logging.handlers import RotatingFileHandler

# Configure logging
log_file = os.path.join(LOGS_DIR, 'db_manager.log')
logger = logging.getLogger('db_manager')
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(
    log_file, maxBytes=5 * 1024 * 1024, backupCount=5
)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(handler)

DB_PATH = os.path.join(BASE_DIR, 'data', 'trading_bot.db')

def initialize_db():
    if not os.path.exists(os.path.join(BASE_DIR, 'data')):
        os.makedirs(os.path.join(BASE_DIR, 'data'))

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Create tables
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            strategy TEXT,
            buy_timestamp TEXT,
            buy_price REAL,
            buy_amount REAL,
            sell_timestamp TEXT,
            sell_price REAL,
            sell_amount REAL,
            profit_loss REAL,
            order_id TEXT  -- Added order_id to store the exchange's order ID
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            error_message TEXT
        )
    ''')
    # Existing table for Strategy 1 data capture
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS strategy1_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            timestamp TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            PSAR REAL,
            PSAR_trend INTEGER,
            STOCH_K_9_3 REAL,
            STOCH_K_14_3 REAL,
            STOCH_K_40_4 REAL,
            STOCH_K_60_10_1 REAL,
            BB_percent_b REAL
        )
    ''')

    # Create table for Strategy 2 data
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
    conn.close()

def log_strategy1_data(data):
    """
    Log data for Strategy 1 to the database.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO strategy1_data (
            ticker,
            timestamp,
            open,
            high,
            low,
            close,
            PSAR,
            PSAR_trend,
            STOCH_K_9_3,
            STOCH_K_14_3,
            STOCH_K_40_4,
            STOCH_K_60_10_1,
            BB_percent_b
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        data.get('ticker'),
        data.get('timestamp'),
        data.get('open'),
        data.get('high'),
        data.get('low'),
        data.get('close'),
        data.get('PSAR'),
        data.get('PSAR_trend'),
        data.get('STOCH_K_9_3'),
        data.get('STOCH_K_14_3'),
        data.get('STOCH_K_40_4'),
        data.get('STOCH_K_60_10_1'),
        data.get('BB_percent_b')
    ))
    conn.commit()
    conn.close()

def log_strategy2_data(data):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    columns = ', '.join(data.keys())
    placeholders = ', '.join('?' * len(data))
    sql = f'INSERT INTO strategy2_data ({columns}) VALUES ({placeholders})'
    cursor.execute(sql, list(data.values()))

    conn.commit()
    conn.close()

def update_strategy2_data(ticker, timestamp, data):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    set_clause = ', '.join(f"{key}=?" for key in data.keys() if key not in ['ticker', 'timestamp'])
    sql = f'''
        UPDATE strategy2_data
        SET {set_clause}
        WHERE ticker=? AND timestamp=?
    '''
    params = [data[key] for key in data if key not in ['ticker', 'timestamp']]
    params.extend([ticker, timestamp])

    cursor.execute(sql, params)
    conn.commit()
    conn.close()

def log_trade_open(ticker, strategy, side, price, amount, order_id):
    """
    Log the opening of a trade in the database.

    :param ticker: str, the trading pair
    :param strategy: str, the strategy name
    :param side: str, 'BUY' or 'SELL'
    :param price: float, the price at which the trade was initiated
    :param amount: float, the amount bought or sold
    :param order_id: str, the exchange's order ID
    :return: int, the database ID of the trade record
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO trades (
            ticker, strategy, buy_timestamp, buy_price, buy_amount, order_id
        ) VALUES (?, ?, ?, ?, ?, ?)
    ''', (
        ticker,
        strategy,
        datetime.utcnow().isoformat(),
        price,
        amount,
        order_id
    ))
    conn.commit()
    trade_id = cursor.lastrowid
    conn.close()
    return trade_id

def log_trade_close(trade_id, sell_price, sell_amount, profit_loss):
    """
    Update the trade record with closing details.

    :param trade_id: int, the database ID of the trade
    :param sell_price: float, the price at which the trade was closed
    :param sell_amount: float, the amount sold
    :param profit_loss: float, the profit or loss from the trade
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE trades
        SET sell_timestamp = ?, sell_price = ?, sell_amount = ?, profit_loss = ?
        WHERE id = ?
    ''', (
        datetime.utcnow().isoformat(),
        sell_price,
        sell_amount,
        profit_loss,
        trade_id
    ))
    conn.commit()
    conn.close()

def get_latest_buy_trade(ticker, strategy):
    """
    Retrieve the latest open buy trade for a given ticker and strategy.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT * FROM trades
        WHERE ticker = ? AND strategy = ? AND sell_timestamp IS NULL
        ORDER BY id DESC LIMIT 1
    ''', (ticker, strategy))
    trade = cursor.fetchone()
    conn.close()
    if trade:
        return {
            'id': trade[0],
            'ticker': trade[1],
            'strategy': trade[2],
            'buy_timestamp': trade[3],
            'buy_price': trade[4],
            'buy_amount': trade[5],
            'order_id': trade[10]  # Added order_id
        }
    else:
        return None

def update_trade_with_sell_info(trade_id, sell_info):
    """
    Update the trade record with sell information.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE trades
        SET sell_timestamp = ?, sell_price = ?, sell_amount = ?, profit_loss = ?
        WHERE id = ?
    ''', (
        sell_info.get('sell_timestamp'),
        sell_info.get('sell_price'),
        sell_info.get('sell_amount'),
        sell_info.get('profit_loss'),
        trade_id
    ))
    conn.commit()
    conn.close()

def get_trades(strategy):
    """
    Retrieve all trades for a given strategy.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT * FROM trades WHERE strategy = ?
    ''', (strategy,))
    trades = cursor.fetchall()
    conn.close()
    # Convert to list of dictionaries
    trades_list = []
    for trade in trades:
        trades_list.append({
            'id': trade[0],
            'ticker': trade[1],
            'strategy': trade[2],
            'buy_timestamp': trade[3],
            'buy_price': trade[4],
            'buy_amount': trade[5],
            'sell_timestamp': trade[6],
            'sell_price': trade[7],
            'sell_amount': trade[8],
            'profit_loss': trade[9],
            'order_id': trade[10]  # Added order_id
        })
    return trades_list

def log_error(error_message):
    """
    Log an error message to the database.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    timestamp = datetime.utcnow().isoformat()
    cursor.execute('''
        INSERT INTO errors (timestamp, error_message)
        VALUES (?, ?)
    ''', (timestamp, error_message))
    conn.commit()
    conn.close()