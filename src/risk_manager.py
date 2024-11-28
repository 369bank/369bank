# risk_manager.py

from datetime import datetime, timedelta
from db_manager import get_trades
from config.config import MAX_CONSECUTIVE_LOSSES, MAX_NET_LOSS_24H
from notifier import send_email
import logging
import os

# Configure logging
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOGS_DIR = os.path.join(BASE_DIR, 'logs')
if not os.path.exists(LOGS_DIR):
  os.makedirs(LOGS_DIR)

from logging.handlers import RotatingFileHandler

log_file = os.path.join(LOGS_DIR, 'risk_manager.log')
logger = logging.getLogger('risk_manager')
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(
  log_file, maxBytes=5 * 1024 * 1024, backupCount=5
)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
handler.setFormatter(formatter)
if not logger.handlers:
  logger.addHandler(handler)

def check_risk_management(strategy):
  """
  Check if trading should be halted for the strategy.

  :param strategy: str, the strategy name
  :return: bool, True if trading should continue, False if it should halt
  """
  trades = get_trades(strategy)
  # Check for consecutive losses
  consecutive_losses = 0
  for trade in reversed(trades):
      if trade['profit_loss'] is not None and trade['profit_loss'] < 0:
          consecutive_losses += 1
          if consecutive_losses >= MAX_CONSECUTIVE_LOSSES:
              send_email(
                  subject=f"Trading Halted for {strategy}",
                  body=f"Trading halted due to {consecutive_losses} consecutive losses."
              )
              logger.warning(f"Trading halted for {strategy} due to {consecutive_losses} consecutive losses.")
              return False
      else:
          consecutive_losses = 0

  # Check for net loss over 24 hours
  now = datetime.utcnow()
  start_time = now - timedelta(hours=24)
  net_loss = sum(
      trade['profit_loss'] for trade in trades
      if trade['profit_loss'] is not None and datetime.fromisoformat(trade['buy_timestamp']) >= start_time
  )
  if net_loss <= -MAX_NET_LOSS_24H:
      send_email(
          subject=f"Trading Halted for {strategy}",
          body=f"Trading halted due to net loss of ${abs(net_loss)} over the past 24 hours."
      )
      logger.warning(f"Trading halted for {strategy} due to net loss of ${abs(net_loss)} over 24 hours.")
      return False

  return True  # Trading can continue