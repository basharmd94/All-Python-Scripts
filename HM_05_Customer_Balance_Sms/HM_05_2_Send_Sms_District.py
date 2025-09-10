"""
🚀 HM_05_2_Send_Sms_District.py – Send SMS to District Customers with Mobile

📌 PURPOSE:
    - Reads 'With Mobile' sheet from district_customer_balance.xlsx
    - Sends due & payment SMS to district customers with mobile number
    - Sends summary email

🔧 DATA SOURCE:
    - Input: district_customer_balance.xlsx (from HM_04_Customer_Balance_SMS.py)
    - Sheet: 'With Mobile'

🔐 CONFIG:
    - .env must contain:
        SMS_URL=https://api.sms.net.bd/sendsms
        SMS_API_KEY=your_api_key
        SENDER_ID=HMBR

📧 EMAIL:
    - Recipients: get_email_recipients("HM_05_2_Send_Sms_District")
    - Fallback: ithmbrbd@gmail.com

💡 NOTE:
    - Skips customers without mobile or negative balance
    - Uses 7-second delay between messages
    - Excludes no test customers (all real)
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
INPUT_FILE = 'district_customer_balance.xlsx'
SHEET_NAME = 'With Mobile'  # Sheet with customers who have mobile


# === 5. Read and Clean Data ===
print(f"📥 Reading '{SHEET_NAME}' from {INPUT_FILE}...")
try:
    df = pd.read_excel(INPUT_FILE, sheet_name=SHEET_NAME)
except FileNotFoundError:
    raise FileNotFoundError(f"❌ File '{INPUT_FILE}' not found. Run HM_04_Customer_Balance_SMS.py first.")
except ValueError as e:
    if "sheet" in str(e).lower():
        raise ValueError(f"❌ Sheet '{SHEET_NAME}' not found in {INPUT_FILE}")
    else:
        raise e

# Clean mobile number and date
df['xtaxnum'] = df['xtaxnum'].astype(str).replace(r'\.0$', '', regex=True)
df['last_pay_date'] = pd.to_datetime(df['last_pay_date'], errors='coerce').dt.strftime('%Y-%m-%d')

# Filter valid: has mobile, non-negative balance
df = df[
    (df['xtaxnum'].notna()) &
    (df['xtaxnum'] != 'nan') &
    (df['xtaxnum'] != '') &
    (df['balance'] >= 0)
].copy()

print(f"✅ Loaded {len(df)} district customers with mobile and valid balance.")


# === 6. Send SMS to Each Customer ===
status_log = []
for record in df.to_dict('records'):
    try:
        # Clean customer ID and name
        cust_id = record['xcus'].split('-')[1].lstrip('0') or "0"
        cust_name = record['xshort'].split()[0].strip() if record['xshort'] else "Customer"

        # Build message
        msg = (
            f"Dear Customer, ID-{cust_id},\n"
            f"Last Deposit Amt : {record['last_rec_amt']:,.2f} Tk\n"
            f"On: {record['last_pay_date']}\n"
            f"Current Due {record['balance']:,.2f} Tk"
        )

        # Ensure mobile has 88 prefix
        mobile = record['xtaxnum']
        if not mobile.startswith('88'):
            mobile = '88' + mobile

        # SMS payload
        payload = {
            'api_key': SMS_API_KEY,
            'msg': msg,
            'to': mobile,
            'sender_id': SENDER_ID
        }

        print(f"📤 Sending to {mobile} | ID-{cust_id}")
        print(f"💬 {msg}")

        # Send request
        response = requests.post(SMS_URL, data=payload, timeout=10)
        status_log.append({
            'xcus': record['xcus'],
            'mobile': mobile,
            'status_code': response.status_code,
            'response': response.text
        })

        print(f"✅ {response.status_code} | {response.text}")
        print(f"⏳ Sleeping 7 seconds...")
        time.sleep(7)

    except Exception as e:
        print(f"❌ Failed to send SMS to {record['xcus']}: {e}")
        status_log.append({
            'xcus': record['xcus'],
            'mobile': record.get('xtaxnum'),
            'status_code': 'ERROR',
            'response': str(e)
        })
        time.sleep(7)


print("✅ All district SMS attempts completed.")


# === 7. Prepare Summary for Email ===
total_sent = len([s for s in status_log if s['status_code'] != 'ERROR' and str(s['status_code'])[0] == '2'])
total_failed = len(status_log) - total_sent

# Summary stats
original_total_collection = df['last_rec_amt'].sum()
original_customer_count = len(df)
payment_date = df['last_pay_date'].iloc[0] if len(df) > 0 else "Unknown"

summary_msg = f"""
District customer SMS report:

📅 Last Payment Date: {payment_date}
👥 Total District Customers: {original_customer_count}
💰 Total Collection: {original_total_collection:,.2f} Tk
📤 SMS Sent: {total_sent}
❌ Failed: {total_failed}

Messages sent to district customers with mobile number.
"""

print(summary_msg)


# === 8. Send Summary Email ===
try:
    recipients = get_email_recipients("HM_05_2_Send_Sms_District")
    print(f"📬 Sending summary to: {recipients}")
except Exception as e:
    print(f"⚠️ Fallback: {e}")
    recipients = ["ithmbrbd@gmail.com"]

send_mail(
    subject="HM_05_2 SMS Report – District Customer Due",
    bodyText=summary_msg.strip(),
    attachment=[],
    recipient=recipients,
    html_body=None
)


print("📨 Summary email sent.")