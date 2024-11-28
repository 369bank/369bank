# auth.py

from cdp.cdp_api_client import CdpApiClient
from config.config import COINBASE_API_KEY, COINBASE_PRIVATE_KEY, SANDBOX_MODE

def get_client():
    if SANDBOX_MODE:
        host = 'https://api-public.sandbox.exchange.coinbase.com'
    else:
        host = 'https://api.exchange.coinbase.com' # or base_url = 'https://api.coinbase.com'

    client = CdpApiClient(
        api_key=COINBASE_API_KEY,
        private_key=COINBASE_PRIVATE_KEY,
        host=host,
        debugging=True  # Optional: Set to True for debugging output
    )

    return client