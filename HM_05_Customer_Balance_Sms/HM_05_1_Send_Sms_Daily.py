"""
🚀 HM_05_1_Send_Sms_Daily.py – Send Daily Customer Due SMS

📌 PURPOSE:
    - Reads customer payment data from 'customer_balance.xlsx'
    - Sends SMS via sms.net.bd for each customer with mobile number
    - Sends summary email to stakeholders

🔧 DATA SOURCE:
    - Input: customer_balance.xlsx (from HM_04_Customer_Balance_SMS.py)
    - SMS API: sms.net.bd (config from .env)

🔐 CONFIG:
    - .env must contain:
        SMS_URL=https://api.sms.net.bd/sendsms
        SMS_API_KEY=your_api_key
        SENDER_ID=HMBR

📧 EMAIL:
    - Recipients: get_email_recipients("HM_05_1_Send_Sms_Daily")
    - Fallback: ithmbrbd@gmail.com

💡 NOTE:
    - Skips customers without mobile or negative balance
    - Excludes specific test customers
    - Waits 15s between messages (API-safe)
"""

import os
import sys
import pandas as pd
import requests
import time
from dotenv import load_dotenv


# === 1. Load Environment Variables from .env ===
load_dotenv()

SMS_URL = os.getenv("SMS_URL")
SMS_API_KEY = os.getenv("SMS_API_KEY")
SENDER_ID = os.getenv("SENDER_ID")

# Validate required config
if not all([SMS_URL, SMS_API_KEY, SENDER_ID]):
    raise EnvironmentError("❌ Missing SMS config in .env: SMS_URL, SMS_API_KEY, SENDER_ID required.")


# === 2. Add root (E:\) to Python path ===
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


# === 3. Import shared modules ===
from mail import send_mail, get_email_recipients


# === 4. Constants ===
INPUT_FILE = 'customer_balance.xlsx'
EXCLUDED_CUSTOMERS = ['CUS-002971', 'CUS-003335', 'CUS-003485']  # Shahalam Sir's test customers


# === 5. Read and Clean Data ===
print(f"📥 Reading data from {INPUT_FILE}...")
try:
    df = pd.read_excel(INPUT_FILE)
except FileNotFoundError:
    raise FileNotFoundError(f"❌ File '{INPUT_FILE}' not found. Run HM_05_Customer_Balance_SMS.py first.")

# Exclude test customers
df = df[~df['xcus'].isin(EXCLUDED_CUSTOMERS)].copy()

# Clean mobile number and date
df['xtaxnum'] = df['xtaxnum'].astype(str).replace(r'\.0$', '', regex=True)
df['last_pay_date'] = pd.to_datetime(df['last_pay_date'], errors='coerce').dt.strftime('%Y-%m-%d')

print(f"✅ Loaded {len(df)} customers after filtering out test accounts.")


# === 6. Filter Valid Customers for SMS ===
df_sms = df[
    (df['xtaxnum'].notna()) &
    (df['xtaxnum'] != 'nan') &
    (df['xtaxnum'] != '') &
    (df['balance'] >= 0)
].copy()

print(f"📩 {len(df_sms)} customers eligible for SMS.")


# === 7. Send SMS to Each Customer ===
status_log = []
for record in df_sms.to_dict('records'):
    try:
        # Clean customer ID and name
        cust_id = record['xcus'].split('-')[1].lstrip('0') or "0"
        cust_name = record['xshort'].split()[0].strip() if record['xshort'] else "Customer"

        # Build message
        msg = (
            f"Dear Customer, ID-{cust_id},\n"
            f"Last Deposit Date : {record['last_pay_date']}\n"
            f"Deposit : {record['last_rec_amt']:,.2f} Tk\n"
            f"Current Due {record['balance']:,.2f} Tk"
        )

        # SMS payload
        payload = {
            'api_key': SMS_API_KEY,
            'msg': msg,
            'to': '88' + record['xtaxnum'] if not record['xtaxnum'].startswith('88') else record['xtaxnum'],
            'sender_id': SENDER_ID
        }

        print(f"📤 Sending to {payload['to']} | Message length: {len(msg)}")
        print(f"💬 {msg}")

        # Send request
        response = requests.post(SMS_URL, data=payload, timeout=10)
        status_log.append({
            'xcus': record['xcus'],
            'mobile': payload['to'],
            'status_code': response.status_code,
            'response': response.text
        })

        print(f"✅ {response.status_code} | {response.text}")
        time.sleep(10)  # Respect API rate limit

    except Exception as e:
        print(f"❌ Failed to send SMS to {record['xcus']}: {e}")
        status_log.append({
            'xcus': record['xcus'],
            'mobile': record.get('xtaxnum'),
            'status_code': 'ERROR',
            'response': str(e)
        })
        time.sleep(10)


print("✅ All messages attempted.")


# === 8. Prepare Summary for Email ===
total_sent = len([s for s in status_log if s['status_code'] != 'ERROR' and str(s['status_code'])[0] == '2'])
total_failed = len(status_log) - total_sent

# Summary from original data (before filtering)
original_total_collection = df['last_rec_amt'].sum()
original_customer_count = len(df)
payment_date = df['last_pay_date'].iloc[0] if len(df) > 0 else "Unknown"

summary_msg = f"""
Customer payment SMS report:

📅 Collection Date: {payment_date}
👥 Total Customers Paid: {original_customer_count}
💰 Total Collection: {original_total_collection:,.2f} Tk
📤 SMS Sent: {total_sent}
❌ Failed: {total_failed}

All messages processed via HMBR SMS Gateway.
"""

print(summary_msg)


# === 9. Send Summary Email ===
try:
    recipients = get_email_recipients("HM_05_1_Send_Sms_Daily")
    print(f"📬 Sending summary to: {recipients}")
except Exception as e:
    print(f"⚠️ Fallback: {e}")
    recipients = ["ithmbrbd@gmail.com"]

send_mail(
    subject="HM_05_1 Daily SMS Report – Customer Payment & Due",
    bodyText=summary_msg.strip(),
    attachment=[],  # No attachment unless needed
    recipient=recipients,
    html_body=None
)


print("📨 Summary email sent.")