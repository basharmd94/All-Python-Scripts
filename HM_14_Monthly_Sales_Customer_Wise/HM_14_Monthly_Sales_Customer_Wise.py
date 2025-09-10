"""
🚀 HM_14_Monthly_Sales_Customer_Wise.py – Monthly Sales Report (Customer-Wise) for Multiple Businesses

📌 PURPOSE:
    - Generate monthly pivot sales report per customer
    - One Excel file with 3 sheets: HMBR, GI-Corp, Zepto
    - Send single email with one attachment
"""

import os
import sys
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


# === 1. Load Environment Variables from .env ===
load_dotenv()

# Load ZIDs
try:
    ZID_GULSHAN_TRADING = int(os.environ["ZID_GULSHAN_TRADING"])
    ZID_GI = int(os.environ["ZID_GI"])
    ZID_ZEPTO_CHEMICALS = int(os.environ["ZID_ZEPTO_CHEMICALS"])
except KeyError as e:
    raise RuntimeError(f"❌ Missing ZID in .env: {e}")


# === 2. Add root (E:\) to Python path ===
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


# === 3. Import shared modules ===
from mail import send_mail, get_email_recipients
from project_config import DATABASE_URL


# === 4. Create engine using shared DATABASE_URL ===
engine = create_engine(DATABASE_URL)


# === 5. Suppress warnings ===
import warnings
warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)
pd.options.mode.chained_assignment = None


# === 6. Date Setup ===
START_DATE = (datetime.now() - relativedelta(years=1)).strftime('%Y-%m-%d')
print(f"📅 Generating reports for sales from: {START_DATE}")


# === 7. Fetch Sales Data ===
def get_sales(engine, zid, start_date):
    """Fetch sales by customer and month."""
    query = text("""
        SELECT 
            opdor.xdate, 
            opdor.xdiv, 
            opdor.xcus, 
            SUM(opdor.xdtwotax) AS total
        FROM opdor
        WHERE opdor.zid = :zid
          AND opdor.xdate >= :start_date
        GROUP BY opdor.xdate, opdor.xcus, opdor.xdiv
        ORDER BY opdor.xdate ASC
    """)
    return pd.read_sql(query, engine, params={'zid': zid, 'start_date': start_date})


# === 8. Fetch Customer Master ===
def get_cacus(engine, zid):
    """Fetch customer details."""
    query = text("""
        SELECT xcus, xshort, xmobile, xadd1, xadd2
        FROM cacus
        WHERE zid = :zid
    """)
    return pd.read_sql(query, engine, params={'zid': zid})


# === 9. Generate Report for One Business ===
def generate_report(engine, zid, business_name, start_date):
    """Generate and return DataFrame for one business."""
    print(f"📊 Generating report for {business_name} (ZID={zid})...")

    # Fetch data
    df_sales = get_sales(engine, zid, start_date)
    df_cus = get_cacus(engine, zid)

    if df_sales.empty:
        print(f"❌ No sales data for {business_name}")
        return pd.DataFrame()

    # Add year/month
    df_sales['xdate'] = pd.to_datetime(df_sales['xdate'])
    df_sales['year'] = df_sales['xdate'].dt.year
    df_sales['month'] = df_sales['xdate'].dt.month

    # Group by div, cus, month, year
    grouped = df_sales.groupby(['xdiv', 'xcus', 'month', 'year'])['total'].sum().reset_index()

    # Add grand total per customer
    grouped['grand_total'] = grouped.groupby('xcus')['total'].transform('sum')

    # Pivot: months as columns
    pivot = pd.pivot_table(
        grouped,
        values='total',
        index=['xcus', 'year', 'xdiv'],
        columns='month',
        aggfunc='sum',
        fill_value=0
    ).reset_index()

    # Add grand total column
    pivot['grand_total'] = pivot.iloc[:, 3:].sum(axis=1)

    # Merge with customer details
    final = pd.merge(pivot, df_cus, on='xcus', how='left')

    # Rename months
    month_names = {
        1: 'January', 2: 'February', 3: 'March', 4: 'April',
        5: 'May', 6: 'June', 7: 'July', 8: 'August',
        9: 'September', 10: 'October', 11: 'November', 12: 'December'
    }
    final.rename(columns=month_names, inplace=True)

    # Reorder columns
    cols = ['xcus', 'year', 'xshort', 'xmobile', 'xdiv', 'xadd2'] + list(month_names.values()) + ['grand_total']
    final = final.reindex(columns=[c for c in cols if c in final.columns])

    return final


# === 10. Business Configuration ===
BUSINESSES = [
    {"zid": ZID_GULSHAN_TRADING, "name": "HMBR"},
    {"zid": ZID_GI, "name": "GI-Corp"},
    {"zid": ZID_ZEPTO_CHEMICALS, "name": "Zepto"}
]


# === 11. Generate All Reports & Save to One Excel ===
OUTPUT_FILE = "HM_14_Monthly_Sales_Customer_Wise.xlsx"
print(f"📁 Writing all reports to: {OUTPUT_FILE}")

with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
    for biz in BUSINESSES:
        df = generate_report(engine, biz["zid"], biz["name"], START_DATE)
        if not df.empty:
            # Use clean sheet name (no spaces/special chars)
            sheet_name = biz["name"].replace("-", "_")[:31]  # Max 31 chars for Excel
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"✅ Sheet '{sheet_name}' added.")
        else:
            print(f"❌ No data for {biz['name']} – skipping sheet.")

print(f"✅ All reports saved to {OUTPUT_FILE}")


# === 12. Send Email ===
try:
    recipients = get_email_recipients(os.path.splitext(os.path.basename(__file__))[0])
    print(f"📬 Recipients: {recipients}")
except Exception as e:
    print(f"⚠️ Fallback: {e}")
    recipients = ['ithmbrbd@gmail.com', 'saleshmbrbd@gmail.com', 'zepto.sales1@gmail.com']

subject = "HM_14 Monthly Sales Reports (Customer Wise) – HMBR, GI-Corp & Zepto"
body_text = "Please find attached the monthly sales reports (customer-wise) for HMBR, GI-Corp, and Zepto. Each business is in a separate sheet."

send_mail(
    subject=subject,
    bodyText=body_text,
    attachment=[OUTPUT_FILE],
    recipient=recipients,
    html_body=None
)

print(f"✅ Email sent with {OUTPUT_FILE}.")


# === 13. Cleanup ===
engine.dispose()
print("✅ HM_14 completed successfully.")