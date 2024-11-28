# webhook_manager.py

import sys
import os
import logging
from cdp.client.models.webhook import WebhookEventType, WebhookEventFilter
from cdp.cdp import Cdp

# Ensure parent directory is in sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from auth import get_client

# Configure logging
logger = logging.getLogger('webhook_manager')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(handler)

# Initialize the CDP client
client = get_client()
Cdp.initialize(client)

def create_webhook():
    notification_uri = "https://your-app.com/webhook"  # Replace with your actual webhook URL
    event_type = WebhookEventType.TRANSFER  # Change as needed
    network_id = "ethereum-goerli"  # Change as needed

    # Optionally, specify event filters
    event_filters = [
        WebhookEventFilter(
            from_address="0xYourAddress",  # Replace with your address
            to_address=None,
            contract_address=None
        )
    ]

    create_webhook_request = {
        "network_id": network_id,
        "event_type": event_type,
        "notification_uri": notification_uri,
        "event_filters": event_filters
    }

    webhook = Cdp.api_clients.webhooks.create_webhook(create_webhook_request)
    logger.info(f"Webhook created: {webhook}")
    return webhook