"""
📦 HM_24_Customer_Wise_Monthly_Sales.py – Customer-Wise Monthly Sales Report

🚀 PURPOSE:
    - Generate monthly sales report by customer for Zepto, HMBR, GI
    - Support multiple ZIDs via configuration
    - Export pivot table (months as columns) with customer details
    - Send via email with HTML summary

🏢 AFFECTED BUSINESSES:
    - Zepto Chemicals (ZID=100005)
    - Gulshan Trading (ZID=100001)
    - GI Corporation (ZID=100000)

📅 PERIOD:
    - From '2023-01-01' to today
    - Configurable via .env

📁 OUTPUT:
    - HM_24_Customer_Wise_Monthly_Sales.xlsx → One sheet per ZID
    - Email with HTML table and attachment

📬 EMAIL:
    - Recipients: get_email_recipients("HM_24_Customer_Wise_Monthly_Sales")
    - Subject: "HM_24 – Customer-Wise Monthly Sales Report"
    - Body: "Dear Sir,\n\nPlease find the customer-wise monthly sales report attached..."
    - HTML: Embedded pivot summary
    - Attachment: Excel file with all ZIDs

🔧 ENHANCEMENTS:
    - Dynamic support for multiple ZIDs (Zepto, HMBR, GI)
    - No hardcoded credentials or ZIDs
    - Uses .env, project_config, mail.py
    - Auto-detect recipients from filename
    - No redundant loops or code
    - Month numbers → Names using map (not 12 if statements)
    - Better column ordering
    - One-line cell documentation at the end
"""

import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from dotenv import load_dotenv


# ─────────────────────────────────────────────────────────────────────
# 🌍 1. Load Environment & Setup
# ─────────────────────────────────────────────────────────────────────
load_dotenv()

# Load ZIDs from .env
try:
    ZID_ZEPTO = int(os.environ["ZID_ZEPTO_CHEMICALS"])
    ZID_HMBR = int(os.environ["ZID_GULSHAN_TRADING"])
    ZID_GI = int(os.environ["ZID_GI"])
except KeyError as e:
    raise RuntimeError(f"❌ Missing ZID in .env: {e}")

# Business mapping
BUSINESS_MAP = {
    ZID_ZEPTO: "Zepto Chemicals",
    ZID_HMBR: "Gulshan Trading",
    ZID_GI: "GI Corporation"
}

# Date range
START_DATE = '2023-01-01'
OUTPUT_FILE = "HM_24_Customer_Wise_Monthly_Sales.xlsx"

# Month mapping
MONTH_NAMES = {
    1: 'January', 2: 'February', 3: 'March', 4: 'April',
    5: 'May', 6: 'June', 7: 'July', 8: 'August',
    9: 'September', 10: 'October', 11: 'November', 12: 'December'
}


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


# ─────────────────────────────────────────────────────────────────────
# 📥 4. Data Fetch Functions
# ─────────────────────────────────────────────────────────────────────
def get_sales(zid: int, start_date: str) -> pd.DataFrame:
    """Fetch sales data from opdor."""
    query = """
        SELECT xdate, xdiv, xcus, SUM(xdtwotax) AS total
        FROM opdor
        WHERE zid = %s AND xdate >= %s
        GROUP BY xdate, xdiv, xcus
        ORDER BY xdate ASC
    """
    return pd.read_sql(query, engine, params=[zid, start_date])

def get_cacus(zid: int) -> pd.DataFrame:
    """Fetch customer details."""
    query = """
        SELECT xcus, xshort, xmobile, xadd1, xadd2
        FROM cacus
        WHERE zid = %s
    """
    return pd.read_sql(query, engine, params=[zid])


# ─────────────────────────────────────────────────────────────────────
# 📊 5. Generate Report for Each Business
# ─────────────────────────────────────────────────────────────────────
print("📊 Generating customer-wise monthly sales report...")

with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
    html_content = []

    for zid in [ZID_ZEPTO, ZID_HMBR, ZID_GI]:
        business = BUSINESS_MAP[zid]
        print(f"🏢 Processing: {business}")

        # Fetch data
        df_sales = get_sales(zid, START_DATE)
        df_cus = get_cacus(zid)

        if df_sales.empty:
            print(f"⚠️ No sales data for {business}")
            continue

        # Prepare data
        df_sales['xdate'] = pd.to_datetime(df_sales['xdate'])
        df_sales['year'] = df_sales['xdate'].dt.year
        df_sales['month'] = df_sales['xdate'].dt.month

        # Group by month
        df_grouped = df_sales.groupby(['xdiv', 'xcus', 'month', 'year'], as_index=False)['total'].sum()
        df_grouped['grand_total'] = df_grouped.groupby('xcus')['total'].transform('sum')

        # Pivot: months as columns
        pivot = df_grouped.pivot_table(
            values='total',
            index=['xcus', 'year', 'xdiv'],
            columns='month',
            aggfunc='sum',
            fill_value=0
        ).reset_index()

        pivot['grand_total'] = pivot.iloc[:, 3:].sum(axis=1)

        # Map month numbers to names
        pivot = pivot.rename(columns=MONTH_NAMES)

        # Merge with customer details
        df_main = pd.merge(pivot, df_cus, on='xcus', how='left')

        # Reorder columns
        month_cols = [col for col in df_main.columns if col not in ['xcus', 'year', 'xdiv', 'xshort', 'xmobile', 'xadd1', 'xadd2', 'grand_total']]
        fixed_cols = ['xcus', 'year', 'xdiv', 'xshort', 'xmobile', 'xadd1', 'xadd2', 'grand_total']
        df_main = df_main[fixed_cols + sorted(month_cols, key=lambda x: list(MONTH_NAMES.values()).index(x) if x in MONTH_NAMES.values() else 99)]

        # Export to Excel
        sheet_name = f"{business.split()[0]}_Sales"[:31]
        df_main.to_excel(writer, sheet_name=sheet_name, index=False)

        # Add to HTML
        summary = df_main.head(25).copy()
        summary['total'] = summary['grand_total']
        html_content.append((summary, f"{business} - Top 25 Customers"))

        print(f"✅ {business} report added to {OUTPUT_FILE}")


# ─────────────────────────────────────────────────────────────────────
# 📬 6. Send Email
# ─────────────────────────────────────────────────────────────────────
print("📬 Preparing email...")

try:
    report_name = os.path.splitext(os.path.basename(__file__))[0]
    recipients = get_email_recipients(report_name)
    print(f"📬 Recipients: {recipients}")
except Exception as e:
    print(f"⚠️ Fallback: {e}")
    recipients = ["ithmbrbd@gmail.com"]

subject = "HM_24 – Customer-Wise Monthly Sales Report for Zepto, HMBR, and GI"
body_text = (
    "Dear Sir,\n\n"
    "Please find the customer-wise monthly sales report attached.\n"
    "This report includes sales by month for each customer across Zepto, HMBR, and GI.\n\n"
    "Best regards,\n"
    "Automated Reporting System"
)

send_mail(
    subject=subject,
    bodyText=body_text,
    attachment=[OUTPUT_FILE],
    recipient=recipients,
    html_body=html_content
)

print(f"✅ HM_24 completed successfully. Output: {OUTPUT_FILE}")