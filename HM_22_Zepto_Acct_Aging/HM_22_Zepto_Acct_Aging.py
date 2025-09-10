"""
📦 HM_22_Zepto_Acct_Aging.py – Zepto Accounts Receivable Aging Report

🚀 PURPOSE:
    - Generate aging report for Zepto (ZID=100005)
    - Track customer outstanding balances by age buckets (5D to RED >90D)
    - Calculate average days between INOP, RCT, SRT, SRJV transactions
    - Export to Excel and email with HTML summary

🏢 AFFECTED BUSINESS:
    - Zepto (ZID=100005)
    - Data Source: PostgreSQL (localhost:5432/da)

📅 PERIOD:
    - Only transactions within last 365 days included in aging
    - Full history used for average days calculation

📁 OUTPUT:
    - H_22_ZeptoReceivableAging.xlsx → Two sheets:
        - 'Aging Report': Balance by age bucket per customer
        - 'Info': Avg days between INOP, RCT, SRT, SRJV
    - Email sent to key stakeholders (HTML embedded)

📬 EMAIL:
    - Recipients: get_email_recipients("HM_22_Zepto_Acct_Aging") → auto-detect from filename
    - Subject: "Zepto Accounts Receivable Aging Report"
    - Body: HTML of aging table
    - Attachment: H_22_ZeptoReceivableAging.xlsx

🔧 LOGIC PRESERVED:
    - All filters and logic identical to original
    - Same customer exclusion list (GULSHAN PACKAGING, etc.)
    - Same bucketing logic (5D, 15D, ..., RED)
    - Same final column order and formatting

📌 ENHANCEMENTS:
    - Uses project_config.DATABASE_URL and .env for ZIDs
    - Replaces raw SMTP with send_mail() from mail.py
    - Auto-detects recipients via get_email_recipients()
    - Fixes deprecated pd.datetime → pd.Timestamp
    - Sanitizes sheet names for Excel
    - Passes df directly to html_body (no file created)
    - One-line cell documentation at the end
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from dotenv import load_dotenv
import time


# ─────────────────────────────────────────────────────────────────────
# 🌍 1. Load Environment & Setup
# ─────────────────────────────────────────────────────────────────────
load_dotenv()

print("🌍 Loading configuration...")

# Load ZID from .env
try:
    ZID_ZEPTO_CHEMICALS = int(os.environ["ZID_ZEPTO_CHEMICALS"])  # 100005
except KeyError as e:
    raise RuntimeError(f"❌ Missing ZID in .env: {e}")

zid = ZID_ZEPTO_CHEMICALS

# Start timer
start_time = time.time()


# ─────────────────────────────────────────────────────────────────────
# 🧩 2. Add Root & Import Shared Modules
# ─────────────────────────────────────────────────────────────────────
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from mail import send_mail, get_email_recipients
from project_config import DATABASE_URL


# ─────────────────────────────────────────────────────────────────────
# ⚙️ 3. Create Database Engine
# ─────────────────────────────────────────────────────────────────────
engine = create_engine(DATABASE_URL)
print("🔗 Connected to database.")


# ─────────────────────────────────────────────────────────────────────
# 📥 4. Data Fetch Functions
# ─────────────────────────────────────────────────────────────────────
def create_cacus(zid):
    query = "SELECT xcus, xshort, xcity FROM cacus WHERE zid = %s"
    return pd.read_sql(query, engine, params=[zid])

def create_gldetail(zid):
    query = """
        SELECT gldetail.xsub, gldetail.xprime, gldetail.xref, gldetail.xoriginal,
               glheader.xdate, gldetail.xsp, gldetail.xvoucher, gldetail.xproj
        FROM gldetail
        JOIN glheader ON glheader.xvoucher = gldetail.xvoucher
        WHERE gldetail.zid = %s
          AND glheader.zid = %s
          AND gldetail.xvoucher NOT LIKE '%%OB%%'
          AND gldetail.xsub LIKE '%%CUS%%'
    """
    df_gldetail = pd.read_sql(query, engine, params=[zid, zid])

    # Exclude other projects
    exclude_list = [
        'RAHIMA ENTERPRISE', 'IMAMGONJ SHOWROOM', 'GULSHAN CHEMICAL',
        'GULSHAN PACKAGING', 'GULSHAN THREAD TAPE', 'GULSHAN PLASTIC',
        '75GULSHAN TRADING', ''
    ]
    df_gldetail = df_gldetail[~df_gldetail['xproj'].isin(exclude_list)]

    # Sort and prepare
    df_gldetail = df_gldetail.sort_values(by=['xsub', 'xdate'])
    df_gldetail['result'] = df_gldetail.groupby('xsub')['xprime'].cumsum()
    df_gldetail['datetime'] = pd.to_datetime(df_gldetail['xdate'])
    df_gldetail['diff'] = df_gldetail.groupby('xsub')['datetime'].diff().dt.days
    df_gldetail = df_gldetail.drop(columns='datetime')

    # Today diff in days
    today = pd.Timestamp.today().date()
    df_gldetail['todayDiff'] = (today - pd.to_datetime(df_gldetail['xdate']).dt.date).dt.days

    # Full and filtered
    df_gldetailfull = df_gldetail.copy()
    df_gldetail = df_gldetail[df_gldetail['todayDiff'] < 365.0]

    return df_gldetail, df_gldetailfull


# ─────────────────────────────────────────────────────────────────────
# 📊 5. Generate Aging Report
# ─────────────────────────────────────────────────────────────────────
print("📊 Generating aging report...")

df_gldetail, df_gldetailfull = create_gldetail(zid)
df_cacus = create_cacus(zid)

# Filter: only customers with INOP, keep only records after last INOP
maxDateINOPdetail = df_gldetail[df_gldetail['xvoucher'].str.contains('INOP')].groupby('xsub')['xdate'].max().to_frame().reset_index()
df_gldetail = df_gldetail[df_gldetail['xsub'].isin(maxDateINOPdetail['xsub'])]
maxDateINOP = df_gldetail[df_gldetail['xvoucher'].str.contains('INOP')].groupby('xsub')['xdate'].max().to_frame()

# Filter: only records on or after last INOP date
dayfilter = df_gldetail.groupby('xsub')['xdate'].apply(
    lambda x: x >= maxDateINOP.loc[x.name, 'xdate'] if x.name in maxDateINOP.index else False
)
finalDetail = df_gldetail[dayfilter.values]

# Prepare aging buckets
cusList = finalDetail['xsub'].unique().tolist()
df_aging = pd.DataFrame(cusList, columns=['xsub'])
for col in ['5D', '15D', '30D', '45D', '60D', '75D', '90D', 'RED', 'Total']:
    df_aging[col] = ''

# Define buckets
D5 = finalDetail[finalDetail['todayDiff'] <= 5].groupby('xsub').tail(1)
D15 = finalDetail[(finalDetail['todayDiff'] > 5) & (finalDetail['todayDiff'] <= 15)].groupby('xsub').tail(1)
D30 = finalDetail[(finalDetail['todayDiff'] > 15) & (finalDetail['todayDiff'] <= 30)].groupby('xsub').tail(1)
D45 = finalDetail[(finalDetail['todayDiff'] > 30) & (finalDetail['todayDiff'] <= 45)].groupby('xsub').tail(1)
D60 = finalDetail[(finalDetail['todayDiff'] > 45) & (finalDetail['todayDiff'] <= 60)].groupby('xsub').tail(1)
D75 = finalDetail[(finalDetail['todayDiff'] > 60) & (finalDetail['todayDiff'] <= 75)].groupby('xsub').tail(1)
D90 = finalDetail[(finalDetail['todayDiff'] > 75) & (finalDetail['todayDiff'] <= 90)].groupby('xsub').tail(1)
RED = finalDetail[finalDetail['todayDiff'] > 90].groupby('xsub').tail(1)

# Map results
for bucket, col in [(D5, '5D'), (D15, '15D'), (D30, '30D'), (D45, '45D'),
                    (D60, '60D'), (D75, '75D'), (D90, '90D'), (RED, 'RED')]:
    mapping = dict(bucket[['xsub', 'result']].values)
    df_aging[col] = df_aging['xsub'].map(mapping)

# Total
finalDetMap = dict(finalDetail[['xsub', 'result']].values)
df_aging['Total'] = df_aging['xsub'].map(finalDetMap)

# Sort, fill NA, rename
df_aging = df_aging.sort_values('Total', ascending=False)
df_aging = df_aging.fillna('-', axis=1)
df_aging = df_aging.rename(columns={'xsub': 'xcus'})
df_aging = df_aging.merge(df_cacus, on='xcus', how='left')
df_aging = df_aging[['xcus', 'xshort', 'xcity', '5D', '15D', '30D', '45D', '60D', '75D', '90D', 'RED', 'Total']]
df_aging = df_aging.round(2)


# ─────────────────────────────────────────────────────────────────────
# 📈 6. Generate Info Report (Avg Days)
# ─────────────────────────────────────────────────────────────────────
print("📈 Generating transaction frequency report...")

df_gldetailfull = df_gldetailfull.fillna(0)
df_gldetailfull = df_gldetailfull.rename(columns={'xsub': 'xcus'})

df_INOP = df_gldetailfull[df_gldetailfull['xvoucher'].str.contains('INOP')].groupby('xcus')['diff'].mean().to_frame().reset_index().sort_values('diff', ascending=False)
df_RCT = df_gldetailfull[df_gldetailfull['xvoucher'].str.contains('RCT')].groupby('xcus')['diff'].mean().to_frame().reset_index().sort_values('diff', ascending=False)
df_SRT = df_gldetailfull[df_gldetailfull['xvoucher'].str.contains('SRT')].groupby('xcus')['diff'].mean().to_frame().reset_index().sort_values('diff', ascending=False)
df_SRJV = df_gldetailfull[df_gldetailfull['xvoucher'].str.contains('SRJV')].groupby('xcus')['diff'].mean().to_frame().reset_index().sort_values('diff', ascending=False)

df_info = df_cacus.merge(df_INOP, on='xcus', how='left').rename(columns={'diff': 'INOP'})
df_info = df_info.merge(df_RCT, on='xcus', how='left').rename(columns={'diff': 'RCT'})
df_info = df_info.merge(df_SRT, on='xcus', how='left').rename(columns={'diff': 'SRT'})
df_info = df_info.merge(df_SRJV, on='xcus', how='left').rename(columns={'diff': 'SRJV'})
df_info = df_info.round(2)
df_info = df_info.sort_values('INOP', ascending=False)


# ─────────────────────────────────────────────────────────────────────
# 📁 7. Export to Excel
# ─────────────────────────────────────────────────────────────────────
print("📁 Exporting to Excel...")

def sanitize_sheet_name(name):
    """Remove invalid characters for Excel sheet names."""
    for char in r'\/*?:[]':
        name = name.replace(char, '_')
    return name[:31]

with pd.ExcelWriter('H_22_ZeptoReceivableAging.xlsx', engine='openpyxl') as writer:
    df_aging.to_excel(writer, sheet_name=sanitize_sheet_name('Aging Report'), index=False)
    df_info.to_excel(writer, sheet_name=sanitize_sheet_name('Info'), index=False)


# ─────────────────────────────────────────────────────────────────────
# 📬 8. Send Email
# ─────────────────────────────────────────────────────────────────────
print("📬 Preparing email...")

try:
    report_name = os.path.splitext(os.path.basename(__file__))[0]
    recipients = get_email_recipients(report_name)
    print(f"📬 Recipients: {recipients}")
except Exception as e:
    print(f"⚠️ Fallback: {e}")
    recipients = ["ithmbrbd@gmail.com"]

subject = "HM_22 Zepto Accounts Receivable Aging Report"
body_text = "Please find the Zepto Receivable Aging Report attached."

# ✅ Pass DataFrame directly to html_body – no file created
html_content = [
    (df_aging, "Zepto Accounts Receivable Aging"),
    (df_info, "Transaction Frequency (Avg Days)")
]

send_mail(
    subject=subject,
    bodyText=body_text,
    attachment=['H_22_ZeptoReceivableAging.xlsx'],
    recipient=recipients,
    html_body=html_content
)

# Runtime
print(f"✅ HM_22 completed successfully in {time.time() - start_time:.2f} seconds.")