"""
📋 HM_17_MO_Details_Last_30_Days.py – 30-Day Manufacturing Order Cost Report

🚀 PURPOSE:
    - Fetch MO details (unit cost) for GI, Zepto, Packaging
    - Show: Item, Description, Std Price, MO No, Date, Unit Cost
    - Unit Cost = (SUM(xqty * xrate) / xqtyprd)
    - Last 30 days only
    - Export to Excel + HTML email

🏢 COMPANIES:
    - GI Corporation (ZID=100000)
    - Zepto Chemicals (ZID=100005)
    - Gulshan Packaging (ZID=100009)

📁 OUTPUT:
    - HM_17_MO_Details_Last_30_Days.xlsx (3 sheets)
    - Email with embedded HTML tables

📬 EMAIL:
    - Recipients: get_email_recipients("HM_17_MO_Details_Last_30_Days")
    - Fallback: ithmbrbd@gmail.com

📅 PERIOD: Last 30 days
"""

import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


# ─────────────────────────────────────────────────────────────────────
# 🌍 1. Load Environment & Setup
# ─────────────────────────────────────────────────────────────────────
load_dotenv()

print("🌍 Loading configuration...")

# Load ZIDs from .env
try:
    ZID_GI = int(os.environ["ZID_GI"])
    ZID_ZEPTO = int(os.environ["ZID_ZEPTO_CHEMICALS"])
    ZID_PACKAGING = int(os.environ["ZID_GULSHAN_PACKAGING"])
except KeyError as e:
    raise RuntimeError(f"❌ Missing ZID in .env: {e}")

# List of ZIDs to process
zid_list = [ZID_GI, ZID_ZEPTO, ZID_PACKAGING]
zid_to_name = {
    ZID_GI: "GI Corporation",
    ZID_ZEPTO: "Zepto Chemicals",
    ZID_PACKAGING: "Gulshan Packaging"
}

# Date Range: Last 30 days
start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
print(f"📅 Reporting Period: {start_date} → Today")


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
# 📥 4. Fetch MO Details
# ─────────────────────────────────────────────────────────────────────
def get_mo_details(zid, start_date):
    """Fetch MO details with unit cost."""
    query = text("""
        SELECT 
            moord.xitem, 
            caitem.xdesc,
            caitem.xstdprice, 
            moord.zid, 
            moord.xmoord,
            moord.xdatemo,
            SUM((moodt.xqty * moodt.xrate) / NULLIF(moord.xqtyprd, 0)) AS unit_cost
        FROM moord
        JOIN moodt ON moord.xmoord = moodt.xmoord
        JOIN caitem ON moord.xitem = caitem.xitem
        WHERE moord.zid = :zid
          AND moodt.zid = :zid
          AND caitem.zid = :zid
          AND moord.xdatemo >= :start_date
        GROUP BY moord.xitem, caitem.xdesc, caitem.xstdprice, moord.zid, moord.xmoord, moord.xdatemo
        ORDER BY caitem.xdesc ASC, moord.xdatemo DESC
    """)
    df = pd.read_sql(query, engine, params={'zid': zid, 'start_date': start_date})
    if not df.empty:
        df['unit_cost'] = df['unit_cost'].round(2)
        df = df[['xitem', 'xdesc', 'xstdprice', 'xmoord', 'xdatemo', 'unit_cost']]
        df = df.rename(columns={
            'xmoord': 'MO Number',
            'xdatemo': 'MO Date',
            'unit_cost': 'Unit Cost (BDT)',
            'xstdprice': 'Std Price'
        })
    return df


# ─────────────────────────────────────────────────────────────────────
# 🧮 5. Fetch Data for All ZIDs
# ─────────────────────────────────────────────────────────────────────
print("📊 Fetching MO data for 3 businesses...")
all_dfs = {}

for zid in zid_list:
    name = zid_to_name[zid]
    print(f"📥 {name} (ZID={zid})...")
    df = get_mo_details(zid, start_date)
    if df.empty:
        print(f"❌ No MO data for {name}")
        df = pd.DataFrame(columns=['xitem', 'xdesc', 'Std Price', 'MO Number', 'MO Date', 'Unit Cost (BDT)'])
    all_dfs[name] = df


# ─────────────────────────────────────────────────────────────────────
# 📁 6. Export to Excel
# ─────────────────────────────────────────────────────────────────────
OUTPUT_FILE = "HM_17_MO_Details_Last_30_Days.xlsx"

print(f"📁 Writing to {OUTPUT_FILE}...")
with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
    for name, df in all_dfs.items():
        # Clean sheet name (max 31 chars)
        sheet_name = name.replace(" ", "_")[:31]
        df.to_excel(writer, sheet_name=sheet_name, index=False)

# Auto column width
from openpyxl import load_workbook
wb = load_workbook(OUTPUT_FILE)
for sheet in wb:
    for col in sheet.columns:
        max_length = min(max(len(str(cell.value)) for cell in col) + 2, 50)
        sheet.column_dimensions[col[0].column_letter].width = max_length
wb.save(OUTPUT_FILE)

print(f"✅ Excel saved: {OUTPUT_FILE}")


# ─────────────────────────────────────────────────────────────────────
# 📬 7. Send Email with HTML Tables
# ─────────────────────────────────────────────────────────────────────
try:
    recipients = get_email_recipients(os.path.splitext(os.path.basename(__file__))[0])
    print(f"📬 Recipients: {recipients}")
except Exception as e:
    print(f"⚠️ Fallback: {e}")
    recipients = ["ithmbrbd@gmail.com"]  # Only fallback

subject = f"HM_17 MO Details – Last 30 Days – {start_date} to {datetime.now().strftime('%Y-%m-%d')}"
body_text = """


Please find the manufacturing order (MO) details for the last 30 days.\n\n

Includes:\n\n
- Item, Description, Std Price\n\n
- MO Number, Date\n\n
- Calculated Unit Cost\n\n

See attached Excel and HTML preview below.\n\n
"""

# Prepare HTML tables
# Prepare HTML tables: Pass DataFrame + Title (let send_mail handle styling)
html_tables = []
for name, df in all_dfs.items():
    html_tables.append((df, f"🔧 {name} MO Details"))

send_mail(
    subject=subject,
    bodyText=body_text,
    attachment=[OUTPUT_FILE],
    recipient=recipients,
    html_body=html_tables
)

print("✅ HM_17 completed successfully.")