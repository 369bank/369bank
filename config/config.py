# config.py

import os
from dotenv import load_dotenv
import logging

# Set up logging
logger = logging.getLogger('config')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
handler.setFormatter(formatter)
if not logger.handlers:
  logger.addHandler(handler)

# Define the base directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Load .env file
DOTENV_PATH = os.path.join(BASE_DIR, '..', '.env')
logger.info(f"Loading .env file from: {DOTENV_PATH}")
load_dotenv(dotenv_path=DOTENV_PATH)

# Fetch environment variables
COINBASE_API_KEY = os.getenv('COINBASE_API_KEY')
COINBASE_PRIVATE_KEY = os.getenv('COINBASE_PRIVATE_KEY')  # Updated
SANDBOX_MODE = os.getenv('SANDBOX_MODE', 'True').lower() == 'true'
EMAIL_ADDRESS = os.getenv('EMAIL_ADDRESS')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')
TWILIO_ACCOUNT_SID = os.getenv('TWILIO_ACCOUNT_SID')
TWILIO_AUTH_TOKEN = os.getenv('TWILIO_AUTH_TOKEN')
TWILIO_PHONE_NUMBER = os.getenv('TWILIO_PHONE_NUMBER')
PHONE_NUMBER = os.getenv('PHONE_NUMBER')
PHONE_NUMBER = os.getenv('PHONE_NUMBER')



# Database file path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_FILE = os.path.join(BASE_DIR, 'data', 'trading_bot.db')  # Adjust the path as needed

# Log environment variables
logger.info(f"COINBASE_API_KEY: {'Loaded' if COINBASE_API_KEY else 'Missing'}")
logger.info(f"COINBASE_PRIVATE_KEY: {'Loaded' if COINBASE_PRIVATE_KEY else 'Missing'}")  # Updated
logger.info(f"SANDBOX_MODE: {SANDBOX_MODE}")
logger.info(f"EMAIL_ADDRESS: {'Loaded' if EMAIL_ADDRESS else 'Missing'}")
logger.info(f"EMAIL_PASSWORD: {'Loaded' if EMAIL_PASSWORD else 'Missing'}")
logger.info(f"PHONE_NUMBER: {'Loaded' if PHONE_NUMBER else 'Missing'}")

# Validate environment variables
missing_vars = []
if not COINBASE_API_KEY:
  missing_vars.append('COINBASE_API_KEY')
if not COINBASE_PRIVATE_KEY:  # Updated
  missing_vars.append('COINBASE_PRIVATE_KEY')
if not EMAIL_ADDRESS:
  missing_vars.append('EMAIL_ADDRESS')
if not EMAIL_PASSWORD:
  missing_vars.append('EMAIL_PASSWORD')
if not PHONE_NUMBER:
  missing_vars.append('PHONE_NUMBER')

if missing_vars:
  for var in missing_vars:
      logger.error(f"{var} is missing in the environment variables.")
  raise ValueError("Missing one or more required environment variables.")
else:
  logger.info("All required environment variables loaded successfully.")

# Strategy 1 Configuration
STRATEGY_1 = {
  'stochastic_levels': {
      'STOCH_K_9_3': 40,
      'STOCH_K_14_3': 40,
      'STOCH_K_40_4': 50,
      'STOCH_K_60_10_1': 50
  },
  'bb_percent_b_threshold': 40.0,
  'default_buy_amount': 100  # Default amount in USD or equivalent
}

CAPTURE_STRATEGY1_DATA = True  # Toggle data capture for Strategy 1

# Strategy 2 Configuration
STRATEGY_2 = {
  'checkpoints': [
      {'minute': 5, 'threshold': 90.0},
      {'minute': 10, 'threshold': 95.0},
      {'minute': 15, 'threshold': 95.0},
      {'minute': 20, 'threshold': 95.0},
      {'minute': 25, 'threshold': 95.0},
      {'minute': 30, 'threshold': 95.0},
      {'minute': 40, 'threshold': 95.0},
      {'minute': 50, 'threshold': 94.0},
  ],
  'default_buy_amount': 10,  # Default amount in USD or equivalent
  'sell_after_candles': 8  # Sell after 8th 1-hour candle
}

# Scenario configuration ('A' or 'B') - A immediate, B starts on the hour.
SCENARIO = 'A'  # Change to 'B' as needed

# List of tickers
TICKERS = [
    'LTC-USD', 'PEPE-USD', 'SHIB-USD', 'ETH-USD', 'DOGE-USD',
    'SOL-USD', 'XRP-USD', 'BTC-USD', 'SUI-USD', 'XLM-USD',
    'ADA-USD', 'LINK-USD', 'AVAX-USD'
]

BUY_AMOUNTS = {

    'LTC-USD': 10,       # Amount in USD
    'PEPE-USD': 10,       # Amount in USD
    'SHIB-USD': 10,       # Amount in USD
    'ETH-USD': 10,       # Amount in USD
    'DOGE-USD': 10,       # Amount in USD
    'SOL-USD': 10,       # Amount in USD
    'XRP-USD': 10,       # Amount in USD
    'BTC-USD': 10,       # Amount in USD
    'SUI-USD': 10,       # Amount in USD
    'XLM-USD': 10,       # Amount in USD
    'ADA-USD': 10,       # Amount in USD
    'AVAX-USD': 10     # Amount in USD
}

# TICKERS = ['LINK-USDC', 'BTC-USD', 'USDT-USD', 'ETH-BTC'] This is the Sandbox only.

# Risk Management Parameters
MAX_CONSECUTIVE_LOSSES = 5  # Default to 5
MAX_NET_LOSS_24H = 150  # Default to $150