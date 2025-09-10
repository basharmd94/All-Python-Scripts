"""
🚀 HM_05_Customer_Balance_SMS.py – Daily Customer Balance Report for SMS

📌 PURPOSE:
    - Fetch customer balance, last payment, and segment by district/mobile.
    - Export to Excel: customer_balance.xlsx, district_customer_balance.xlsx
    - Send via email for SMS processing.

🔧 DATA SOURCES:
    - GL: glheader, gldetail
    - Master: cacus (customer)
    - Database: PostgreSQL via DATABASE_URL in project_config.py

📅 SCHEDULE:
    Runs daily (skips Fridays and holidays from holiday.py)

🏢 INPUT:
    - ZID_GULSHAN_TRADING from .env
    - Project name: "GULSHAN TRADING"

📧 EMAIL:
    - Recipients: get_email_recipients("HM_05_Customer_Balance_SMS")
    - Fallback: ithmbrbd@gmail.com

💡 NOTE:
    - Exits early on Friday or holiday.
    - Uses parameterized queries.
    - Matches HM_02/HM_03 style.
"""

import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
from dotenv import load_dotenv


# === 1. Load Environment Variables from .env ===
load_dotenv()

# Read ZID for GULSHAN TRADING
try:
    ZID = int(os.environ["ZID_GULSHAN_TRADING"])
except KeyError:
    raise RuntimeError("❌ Environment variable ZID_GULSHAN_TRADING not found in .env")
except ValueError:
    raise ValueError("❌ ZID_GULSHAN_TRADING must be a valid integer")

PROJECT_NAME = "GULSHAN TRADING"
print(f"📌 Processing for: {PROJECT_NAME} (ZID={ZID})")


# === 2. Add root (E:\) to Python path ===
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


# === 3. Import shared modules ===
from mail import send_mail, get_email_recipients
from project_config import DATABASE_URL, holiday


# === 4. Create engine using shared DATABASE_URL ===
engine = create_engine(DATABASE_URL)


# === 5. Suppress warnings ===
import warnings
warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)
pd.set_option('display.float_format', '{:.2f}'.format)


# === 6. Holiday & Friday Check ===
def is_today_holiday(holiday_list):
    """Check if today is a holiday."""
    today = datetime.now().date().strftime("%Y-%m-%d")
    return today in holiday_list

def is_today_friday():
    """Check if today is Friday."""
    return datetime.now().weekday() == 4  # Monday=0, Friday=4

# Exit if holiday or Friday
if is_today_holiday(holiday()):
    print("📅 Today is a holiday. Exiting.")
    sys.exit(0)

if is_today_friday():
    print("📅 Today is Friday. Exiting.")
    sys.exit(0)


# === 7. Date Logic ===
# Use 1 or 2 days ago depending on Friday
reference_date = datetime.now() - timedelta(days=2 if (datetime.now() - timedelta(days=1)).strftime("%A") == "Friday" else 1)
TWO_DAYS_AGO = reference_date.strftime("%Y-%m-%d")
CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")

print(f"📆 Reference date for payment: {TWO_DAYS_AGO}")
print(f"📆 Current date: {CURRENT_DATE}")


# === 8. SQL Query Functions (Parameterized) ===
def get_balance(zid, till_date):
    """Fetch customer balance up to given date."""
    query = """
        SELECT 
            gldetail.xsub AS xcus,
            cacus.xshort,
            cacus.xstate,
            cacus.xtaxnum,
            SUM(gldetail.xprime) AS balance
        FROM cacus
        JOIN gldetail ON gldetail.xsub = cacus.xcus
        JOIN glheader ON gldetail.xvoucher = glheader.xvoucher
        WHERE glheader.zid = %(zid)s
          AND gldetail.zid = %(zid)s
          AND cacus.zid = %(zid)s
          AND gldetail.xproj = %(project)s
          AND gldetail.xvoucher NOT LIKE '%%OB%%'
          AND glheader.xdate <= %(till_date)s
        GROUP BY gldetail.xsub, cacus.xshort, cacus.xtaxnum, cacus.xstate
    """
    return pd.read_sql(query, engine, params={
        'zid': zid,
        'project': PROJECT_NAME,
        'till_date': till_date
    })


def get_payment(zid):
    """Fetch all customer payments with date and amount."""
    query = """
        SELECT 
            glheader.xdate AS last_pay_date,
            gldetail.xsub AS xcus,
            gldetail.xamount AS last_rec_amt
        FROM glheader
        JOIN gldetail ON glheader.xvoucher = gldetail.xvoucher
        WHERE glheader.zid = %(zid)s
          AND gldetail.zid = %(zid)s
          AND gldetail.xsub LIKE '%%CUS%%'
          AND (
            glheader.xvoucher LIKE '%%RCT%%' OR
            glheader.xvoucher LIKE 'JV--%%' OR
            glheader.xvoucher LIKE 'CRCT%%' OR
            glheader.xvoucher LIKE 'STJV%%' OR
            glheader.xvoucher LIKE 'BRCT%%'
          )
        ORDER BY gldetail.xsub, glheader.xdate
    """
    return pd.read_sql(query, engine, params={'zid': zid})


# === 9. Fetch Data ===
print("📥 Fetching customer balance...")
df_balance = get_balance(ZID, CURRENT_DATE)

print("📥 Fetching payment history...")
df_payment = get_payment(ZID)
df_payment['last_pay_date'] = pd.to_datetime(df_payment['last_pay_date'])


# === 10. Process Last Payment per Customer ===
last_payment = (
    df_payment.groupby('xcus')
    .agg({'last_pay_date': 'max', 'last_rec_amt': 'last'})
    .reset_index()
)


# === 11. Merge Balance with Last Payment ===
df_full = pd.merge(df_balance, last_payment, on='xcus', how='left')


# === 12. Filter: Last Day Payment (TWO_DAYS_AGO) ===
df_last_payment = df_full[df_full['last_pay_date'].dt.strftime('%Y-%m-%d') == TWO_DAYS_AGO].copy()
df_last_payment = df_last_payment.reset_index(drop=True)


# === 13. District Customer Filters ===
# With mobile (xtaxnum not empty)
df_district_with_mobile = df_full[
    (df_full['balance'] > 500) &
    (df_full['xstate'] == 'District') &
    (df_full['xtaxnum'].notna()) &
    (df_full['xtaxnum'] != '')
].copy().reset_index(drop=True)

# Without mobile filter (only balance & district)
df_district_without_mobile = df_full[
    (df_full['balance'] > 500) &
    (df_full['xstate'] == 'District')
].copy().reset_index(drop=True)


# === 14. Export to Excel ===
with pd.ExcelWriter('customer_balance.xlsx', engine='openpyxl') as writer:
    df_last_payment.to_excel(writer, sheet_name='Last Payment', index=False)

with pd.ExcelWriter('district_customer_balance.xlsx', engine='openpyxl') as writer:
    df_district_with_mobile.to_excel(writer, sheet_name='With Mobile', index=False)
    df_district_without_mobile.to_excel(writer, sheet_name='All District', index=False)


# === 15. Send Email ===
try:
    recipients = get_email_recipients(os.path.splitext(os.path.basename(__file__))[0])
    print(f"📬 Recipients: {recipients}")
except Exception as e:
    print(f"⚠️ Failed to get recipients: {e}. Using fallback.")
    recipients = ["ithmbrbd@gmail.com"]

subject = f"HM_05 Customer Balance for SMS -> {PROJECT_NAME}"
body_text = (
    "Please find attached:\n"
    "1. customer_balance.xlsx – Customers who paid 2 days ago.\n"
    "2. district_customer_balance.xlsx – District customers with balance > 500."
)

send_mail(
    subject=subject,
    bodyText=body_text,
    attachment=['customer_balance.xlsx', 'district_customer_balance.xlsx'],
    recipient=recipients,
    html_body=None
)


# === 16. Cleanup ===
engine.dispose()