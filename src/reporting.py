# reporting.py
import sqlite3
from datetime import datetime, timedelta
from notifier import send_email, send_sms

def send_daily_report():
    # Connect to the database
    conn = sqlite3.connect('src/trading_bot.db')
    cursor = conn.cursor()

    # Calculate the time range for the last 24 hours
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=24)

    # Fetch the number of trades
    cursor.execute("""
        SELECT COUNT(*) FROM trades
        WHERE timestamp BETWEEN ? AND ?
    """, (start_time.isoformat(), end_time.isoformat()))
    trade_count = cursor.fetchone()[0]

    # Fetch the total P&L
    cursor.execute("""
        SELECT SUM(pnl) FROM trades
        WHERE timestamp BETWEEN ? AND ?
    """, (start_time.isoformat(), end_time.isoformat()))
    total_pnl = cursor.fetchone()[0] or 0

    conn.close()

    # Prepare the report
    subject = "Daily P&L Report"
    body = f"Date: {end_time.strftime('%Y-%m-%d')}\nNumber of Trades: {trade_count}\nTotal P&L: ${total_pnl:.2f}"

    # Send notifications
    send_email(subject, body)
    send_sms(body)