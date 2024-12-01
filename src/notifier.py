#notifier.py
import smtplib
from email.mime.text import MIMEText
from config.config import EMAIL_ADDRESS, EMAIL_PASSWORD, PHONE_NUMBER
import logging
import os
from twilio.rest import Client
from config.config import TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER, PHONE_NUMBER

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
    client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    try:
        message = client.messages.create(
            body=message,
            from_=TWILIO_PHONE_NUMBER,
            to=PHONE_NUMBER
        )
        logging.info(f"SMS sent to {PHONE_NUMBER}: {message.sid}")
    except Exception as e:
        logging.error(f"Failed to send SMS: {e}")