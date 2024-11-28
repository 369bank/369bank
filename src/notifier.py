#notifier.py
import smtplib
from email.mime.text import MIMEText
from config.config import EMAIL_ADDRESS, EMAIL_PASSWORD, PHONE_NUMBER
import logging
import os

# Configure logging
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOGS_DIR = os.path.join(BASE_DIR, 'logs')
if not os.path.exists(LOGS_DIR):
    os.makedirs(LOGS_DIR)

logging.basicConfig(
    filename=os.path.join(LOGS_DIR, 'notifier.log'),
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

def send_email(subject, body):
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = EMAIL_ADDRESS
    msg['To'] = EMAIL_ADDRESS

    try:
        # Send the email via SMTP server
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.send_message(msg)
        logging.info(f"Email sent: {subject}")
    except Exception as e:
        logging.error(f"Failed to send email: {e}")

def send_sms(message):
    """
    Send SMS using email-to-SMS gateway.

    Note: This depends on the recipient's mobile carrier.
    Adjust the gateway domain as per the recipient's carrier.
    """
    # Replace with the correct email-to-SMS gateway domain for your carrier
    carrier_gateway = 'optusmobile.com.au'  # Example for Optus in Australia
    sms_address = f"{PHONE_NUMBER}@{carrier_gateway}"

    msg = MIMEText(message)
    msg['Subject'] = ''
    msg['From'] = EMAIL_ADDRESS
    msg['To'] = sms_address

    try:
        # Send the SMS via SMTP server
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.send_message(msg)
        logging.info(f"SMS sent to {PHONE_NUMBER}: {message}")
    except Exception as e:
        logging.error(f"Failed to send SMS: {e}")