"""
🚀 HM_13_Last_MO_Stock.py – Last Manufacturing Order & Stock Report

📌 PURPOSE:
    - For each item: get last MO (before today), MO date, quantity, and current stock
    - Calculate days since last production
    - Export to Excel + send HTML email
"""

import os
import sys
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


# === 1. Load Environment Variables from .env ===
load_dotenv()

try:
    ZID = int(os.environ["ZID_GI"])
except KeyError:
    raise RuntimeError("❌ ZID_GI not found in .env")

print(f"📌 Processing for ZID={ZID} (GI Corporation)")


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
TODAY_STR = datetime.now().strftime("%Y-%m-%d")


# === 7. SQL Query: Last MO + Stock + Days Since ===
query = text("""
WITH LastMO AS (
    SELECT DISTINCT ON (m.xitem)
        m.xitem,
        m.xmoord AS last_mo_number,
        m.xdatemo AS last_mo_date,
        m.xqtyprd AS last_mo_qty,
        m.xunit,
        c.xdesc AS item_description
    FROM 
        moord m
        LEFT JOIN caitem c ON m.xitem = c.xitem AND m.zid = c.zid
    WHERE 
        m.zid = :zid
        AND m.xdatemo < CURRENT_DATE
    ORDER BY 
        m.xitem,
        m.xdatemo DESC,
        m.xmoord DESC
),
StockInfo AS (
    SELECT
        xitem,
        COALESCE(SUM(xqty * xsign), 0) AS stock
    FROM imtrn
    WHERE zid = :zid
    GROUP BY xitem
)
SELECT
    lmo.xitem,
    lmo.item_description,
    lmo.xunit,
    COALESCE(s.stock, 0) AS stock,
    lmo.last_mo_number,
    lmo.last_mo_date,
    lmo.last_mo_qty,
    (CURRENT_DATE - lmo.last_mo_date) AS days_since_last_mo
FROM
    LastMO lmo
    LEFT JOIN StockInfo s ON lmo.xitem = s.xitem
ORDER BY
    days_since_last_mo DESC;
""")

print("📊 Fetching last MO and stock data...")
df_report = pd.read_sql(query, engine, params={'zid': ZID})

if df_report.empty:
    raise ValueError("❌ No data returned from query. Check ZID or view permissions.")

print(f"✅ Fetched {len(df_report)} items.")


# === 8. Export to Excel with Auto Column Width ===
OUTPUT_FILE = f"H_13_Last_MO_Stock_GI.xlsx"

with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
    df_report.to_excel(writer, sheet_name='Last MO & Stock', index=False)

from openpyxl import load_workbook
wb = load_workbook(OUTPUT_FILE)
ws = wb['Last MO & Stock']

# Auto-adjust column width
for col in ws.columns:
    max_length = 0
    col_letter = col[0].column_letter
    for cell in col:
        try:
            if len(str(cell.value)) > max_length:
                max_length = len(str(cell.value))
        except:
            pass
    ws.column_dimensions[col_letter].width = min(max_length + 2, 50)  # Cap at 50

wb.save(OUTPUT_FILE)
print(f"✅ Exported: {OUTPUT_FILE}")


# === 9. Send Email ===
try:
    recipients = get_email_recipients(os.path.splitext(os.path.basename(__file__))[0])
    print(f"📬 Recipients: {recipients}")
except Exception as e:
    print(f"⚠️ Fallback: {e}")
    recipients = ['ithmbrbd@gmail.com', ]

subject = f"HM_13 Last MO & Stock Report – {TODAY_STR}"
body_text = "Please find the latest report showing the last manufacturing order and current stock levels for all items."

html_df_list = [(df_report.head(30), "Last MO & Stock Report")]

send_mail(
    subject=subject,
    bodyText=body_text,
    attachment=[OUTPUT_FILE],
    recipient=recipients,
    html_body=html_df_list
)

print("✅ Report generated and email sent successfully.")