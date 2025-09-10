"""
🚀 HM_11_Mfg_Sale_Stock.py – Daily Mfg, Sales & Stock Report for GI

📌 PURPOSE:
    - Get last day & monthly manufacturing (MO)
    - Get last day & monthly sales
    - Get current inventory balance
    - Merge with item master
    - Export to Excel + send HTML email
"""

import os
import sys
import pandas as pd
from datetime import datetime, timedelta
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
now = datetime.now()

# Last date (skip Friday → Thursday)
last_date = now - timedelta(days=2 if now.weekday() == 0 else 1)  # Mon → Fri, others → yesterday
LAST_DATE = last_date.strftime("%Y-%m-%d")
FIRST_DAY_OF_MONTH = now.replace(day=1).strftime("%Y-%m-%d")

print(f"📅 Last date: {LAST_DATE}")
print(f"📅 First of month: {FIRST_DAY_OF_MONTH}")


# === 7. Constants ===
WAREHOUSE = 'Finished Goods Store'
OUTPUT_FILE = f"H_11_Mfg_Sale_Stock_GI.xlsx"


# === 8. Fetch Item Master (xitem, xdesc) ===
query_items = text("""
    SELECT xitem, xdesc 
    FROM caitem 
    WHERE zid = :zid AND xwh = :warehouse
""")
df_items = pd.read_sql(query_items, engine, params={'zid': ZID, 'warehouse': WAREHOUSE})


# === 9. Fetch Manufacturing Data ===

# Last day MO
query_last_mo = text("""
    SELECT xitem, SUM(xqtyprd) AS qty
    FROM moord 
    WHERE zid = :zid AND xstatusmor = 'Completed' AND xdatemo = :last_date
    GROUP BY xitem
""")
df_last_mo = pd.read_sql(query_last_mo, engine, params={'zid': ZID, 'last_date': LAST_DATE})
df_last_mo = df_last_mo.rename(columns={'qty': f'mfg_qty_on_{LAST_DATE}'})

# Monthly MO
query_monthly_mo = text("""
    SELECT xitem, SUM(xqtyprd) AS qty
    FROM moord 
    WHERE zid = :zid AND xstatusmor = 'Completed' 
      AND xdatemo BETWEEN :first_date AND :last_date
    GROUP BY xitem
""")
df_monthly_mo = pd.read_sql(query_monthly_mo, engine, params={
    'zid': ZID,
    'first_date': FIRST_DAY_OF_MONTH,
    'last_date': LAST_DATE
})
df_monthly_mo = df_monthly_mo.rename(columns={'qty': f'mfg_qty_from_{FIRST_DAY_OF_MONTH}'})


# === 10. Fetch Sales Data ===

# Last day sales
query_last_sale = text("""
    SELECT opddt.xitem, SUM(opddt.xqty) AS qty, SUM(opddt.xlineamt) AS amt
    FROM opdor
    JOIN opddt ON opdor.xdornum = opddt.xdornum
    WHERE opdor.zid = :zid AND opddt.zid = :zid
      AND opdor.xdate = :last_date
    GROUP BY opddt.xitem
""")
df_last_sale = pd.read_sql(query_last_sale, engine, params={'zid': ZID, 'last_date': LAST_DATE})
df_last_sale = df_last_sale.rename(columns={'qty': 'last_day_saleqty', 'amt': 'last_day_sales_amt'})

# Monthly sales
query_monthly_sale = text("""
    SELECT opddt.xitem, SUM(opddt.xqty) AS qty, SUM(opddt.xlineamt) AS amt
    FROM opdor
    JOIN opddt ON opdor.xdornum = opddt.xdornum
    WHERE opdor.zid = :zid AND opddt.zid = :zid
      AND opdor.xdate >= :first_date
    GROUP BY opddt.xitem
""")
df_monthly_sale = pd.read_sql(query_monthly_sale, engine, params={
    'zid': ZID,
    'first_date': FIRST_DAY_OF_MONTH
})
df_monthly_sale = df_monthly_sale.rename(columns={'qty': 'monthly_saleqty', 'amt': 'monthly_saleamt'})


# === 11. Fetch Inventory Balance ===
items = tuple(df_items['xitem'].unique())
if len(items) == 1:
    items = f"('{items[0]}')"

query_stock = f"""
    SELECT xitem, SUM(xqty * xsign) AS items_stock_balance 
    FROM imtrn 
    WHERE zid = {ZID} AND xitem IN {items}
    GROUP BY xitem
"""
df_stock = pd.read_sql(query_stock, engine)


# === 12. Merge All DataFrames ===
df_main = df_items
df_main = pd.merge(df_main, df_last_mo, on='xitem', how='left')
df_main = pd.merge(df_main, df_monthly_mo, on='xitem', how='left')
df_main = pd.merge(df_main, df_last_sale, on='xitem', how='left')
df_main = pd.merge(df_main, df_monthly_sale, on='xitem', how='left')
df_main = pd.merge(df_main, df_stock, on='xitem', how='left')

# Fill NaN with 0
df_main = df_main.fillna(0)

# Keep only rows where mfg or sales > 0
mfg_cols = [c for c in df_main.columns if 'mfg_qty' in c]
sale_cols = ['last_day_saleqty', 'monthly_saleqty']
filter_cols = mfg_cols + sale_cols
mask = (df_main[filter_cols] > 0).any(axis=1)
df_main = df_main[mask].sort_values('xitem').reset_index(drop=True)

# Round numeric columns to 1 decimal
num_cols = df_main.select_dtypes(include='number').columns
df_main[num_cols] = df_main[num_cols].round(1)


# === 13. Export to Excel with Auto Column Width ===
with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
    df_main.to_excel(writer, sheet_name='mfg_sale_stock_report', index=False)

from openpyxl import load_workbook
wb = load_workbook(OUTPUT_FILE)
ws = wb['mfg_sale_stock_report']

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
    adjusted_width = min(max_length + 2, 50)
    ws.column_dimensions[col_letter].width = adjusted_width

wb.save(OUTPUT_FILE)
print(f"✅ Exported: {OUTPUT_FILE}")


# === 14. Send Email ===
try:
    recipients = get_email_recipients(os.path.splitext(os.path.basename(__file__))[0])
    print(f"📬 Recipients: {recipients}")
except Exception as e:
    print(f"⚠️ Fallback: {e}")
    recipients = ['ithmbrbd@gmail.com']

subject = f"HM_11 Mfg, Sale & Stock Report – GI – {LAST_DATE}"
body_text = "Please find the daily manufacturing, sales, and stock report."

html_df_list = [(df_main.head(20), "GI Mfg & Sales Summary")]

send_mail(
    subject=subject,
    bodyText=body_text,
    attachment=[OUTPUT_FILE],
    recipient=recipients,
    html_body=html_df_list
)


# === 15. Cleanup ===
engine.dispose()
print("✅ HM_11 completed successfully.")