# test_authentication.py

import sys
import os
import json

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from auth import get_client
from config.config import SANDBOX_MODE

# Get the REST client
client = get_client()

# If SANDBOX_MODE is True, adjust the API base URL
if SANDBOX_MODE:
  client.base_url = 'https://api-public.sandbox.exchange.coinbase.com'

# Make an API call
try:
  # Fetch accounts as an example
  accounts = client.get_accounts()
  # Convert the response to a dictionary
  accounts_dict = accounts.to_dict()
  # Print the response
  print(json.dumps(accounts_dict, indent=2))
except Exception as e:
  print(f"An error occurred: {e}")