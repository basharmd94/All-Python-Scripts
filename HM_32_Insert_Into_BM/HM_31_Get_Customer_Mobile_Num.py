"""
🌟 CUSTOMER ORDER SAMPLING & TRACKING AUTOMATION

📅 Purpose:
Daily script to sample, deduplicate, track & email new customer orders from HMBR & GI business units.
Auto-deletes main_sheet.xlsx after 60 days to reset customer memory.
Emails today’s new customer list to configured recipients.

🧠 Logic Flow:
1. Fetch confirmed/invoiced orders:
   - District: from 4 days ago
   - City (Dhaka): from 1 day ago
2. Sample 5 customers from each region per business (HMBR + GI)
3. Combine → deduplicate by customer_code across both businesses
4. Filter out customers already in main_sheet.xlsx (rolling 60-day memory)
5. Export new customers → today_customer_order.xlsx
6. Append new customers → main_sheet.xlsx
7. Email today’s file if new customers exist

🗃️ Tables Used:
• opdor — Order headers (date, status, SP, customer)
• opddt — Order details (join key)
• cacus — Customer master (code, name, state, mobile)
• prmst — Salesperson master (SP name)

📬 Recipients:
• Email: ithmbrbd@gmail.com,asaddat87@gmail.com,hmbronline@gmail.com
• Subject: HM_31: Customer Order List With Phone Number (District + Retail)
***** Note ****
. For first time delete all excel files from the directory
. Run the script once a day
"""


import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from dotenv import load_dotenv
import warnings

# === Load Environment & Config ===
load_dotenv()
ZID_HMBR = int(os.environ["ZID_GULSHAN_TRADING"])
ZID_GI = int(os.environ["ZID_GI"])
ZID_ZEPTO = int(os.environ["ZID_ZEPTO_CHEMICALS"])  # Not used, kept for reference

PROJECT_ROOT = os.path.dirname(os.getcwd())
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from mail import send_mail, get_email_recipients
from project_config import DATABASE_URL

engine = create_engine(DATABASE_URL)
warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)
pd.set_option('display.float_format', '{:.2f}'.format)


# === Helper: Fetch Order Data ===
def get_order_details(zid, date_param, xstate):
    """Fetch confirmed/invoiced orders for given date and state(s)."""
    query = """
        SELECT 
            opdor.zid,
            opdor.xdate as delivery_date,
            opdor.xsp AS sp_code,
            prmst.xname AS sp_name,
            cacus.xcus AS customer_code,
            cacus.xshort AS customer_name,
            cacus.xstate AS state,
            cacus.xmobile AS mobile_number
        FROM opdor
        LEFT JOIN opddt ON opdor.xdornum = opddt.xdornum
        LEFT JOIN cacus ON opdor.xcus = cacus.xcus
        LEFT JOIN prmst ON opdor.xsp = prmst.xemp
        WHERE opdor.zid = %s
          AND opddt.zid = %s
          AND cacus.zid = %s
          AND prmst.zid = %s
          AND opdor.xdate = %s
          AND (opdor.xstatusdor = '2-Confirmed' OR opdor.xstatusdor = '3-Invoiced')
          AND cacus.xstate IN %s
        GROUP BY opdor.xdornum, opdor.zid, opdor.xdate, opdor.xsp, prmst.xname, 
                 cacus.xcus, cacus.xshort, cacus.xstate, cacus.xmobile
        ORDER BY opdor.xdornum
    """
    return pd.read_sql(query, engine, params=[zid, zid, zid, zid, date_param, xstate])


# === Helper: Sample 5 District + 5 City Orders per Business ===
def get_sampled_orders(zid):
    """Sample 5 District + 5 City orders for given ZID."""
    district_date = (datetime.now() - timedelta(days=4)).strftime('%Y-%m-%d')
    city_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    df_district = get_order_details(zid, district_date, ('District',))
    df_city = get_order_details(zid, city_date, ('Dhaka retail', 'Dhaka General'))

    df_sample_district = df_district.sample(min(5, len(df_district))) if len(df_district) > 0 else pd.DataFrame()
    df_sample_city = df_city.sample(min(5, len(df_city))) if len(df_city) > 0 else pd.DataFrame()

    return pd.concat([df_sample_district, df_sample_city], ignore_index=True)


# === Main Execution ===

# Fetch & sample orders for both businesses
df_hmbr = get_sampled_orders(ZID_HMBR)
df_gi = get_sampled_orders(ZID_GI)

# Combine and deduplicate by customer_code
df_combined = pd.concat([df_hmbr, df_gi], ignore_index=True)
df_order_details = (
    df_combined
    .drop_duplicates(subset=['customer_code'])
    .sort_values(['zid', 'customer_code'])
    .reset_index(drop=True)
)

# Ensure customer_code is string for comparison
df_order_details['customer_code'] = df_order_details['customer_code'].astype(str)


# === Manage main_sheet.xlsx (60-day retention) ===
MAIN_SHEET = "main_sheet.xlsx"
TODAY_FILE = "today_customer_order.xlsx"
CUTOFF_DAYS = 60

if os.path.exists(MAIN_SHEET):
    created = datetime.fromtimestamp(os.path.getctime(MAIN_SHEET))
    if created < datetime.now() - timedelta(days=CUTOFF_DAYS):
        os.remove(MAIN_SHEET)
        print("🗑️  Deleted old main_sheet.xlsx (older than 60 days)")

# Initialize main sheet if missing
if not os.path.exists(MAIN_SHEET):
    pd.DataFrame(columns=df_order_details.columns).to_excel(MAIN_SHEET, index=False)
    existing_customers = []
    print("✅ Created new main_sheet.xlsx")
else:
    df_main = pd.read_excel(MAIN_SHEET, engine='openpyxl')
    existing_customers = (
        df_main['customer_code'].dropna().astype(str).tolist()
        if 'customer_code' in df_main.columns else []
    )
    print(f"📋 Loaded {len(existing_customers)} existing customers")


# === Filter New Customers ===
df_new = df_order_details[~df_order_details['customer_code'].isin(existing_customers)].copy()
print(f"🆕 Found {len(df_new)} new customers")


# === Export & Append ===
if not df_new.empty:
    df_new.to_excel(TODAY_FILE, index=False)
    print(f"💾 Saved to {TODAY_FILE}")

    # Append to main_sheet (pandas 1.3.5 compatible)
    df_existing = pd.read_excel(MAIN_SHEET, engine='openpyxl') if os.path.exists(MAIN_SHEET) else pd.DataFrame()
    df_updated = pd.concat([df_existing, df_new], ignore_index=True)
    df_updated.to_excel(MAIN_SHEET, index=False)
    print(f"📎 Appended to {MAIN_SHEET}")
else:
    print("📭 No new customers today")


# === Email ===
if not df_new.empty:
    try:
        # Extract report name from filename
        report_name = os.path.splitext(os.path.basename(__file__))[0]
        recipients = get_email_recipients(report_name)
        print(f"📬 Recipients: {recipients}")
    except Exception as e:
        print(f"⚠️ Failed to fetch recipients: {e}")
        recipients = ["ithmbrbd@gmail.com"]  # Fallback

    # Send email using resolved recipients (whether default or fetched)
    try:
        send_mail(
            subject="HM_31: Customer Order List With Phone Number (District + Retail)",
            bodyText="Attached are the new customer orders for today.",
            attachment=[TODAY_FILE],
            recipient=recipients
        )
        print("📧 Email sent successfully")
    except Exception as e:
        print(f"❌ Email failed: {e}")

# === Cleanup ===
engine.dispose()
print("✅ Process completed")