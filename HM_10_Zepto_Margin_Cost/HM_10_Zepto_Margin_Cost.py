"""
🚀 H_10_Zepto_Margin_Cost.py – 30-Day Margin Analysis for Zepto

📌 PURPOSE:
    - Calculate gross margin for FZ items sold in last 30 days
    - Use MO cost if produced recently
    - Use last MO cost if not produced
    - Use GRN cost for trading items
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
    ZID = int(os.environ["ZID_ZEPTO_CHEMICALS"])
except KeyError:
    raise RuntimeError("❌ ZID_ZEPTO_CHEMICALS not found in .env")

print(f"📌 Processing margin cost for ZID={ZID} (Zepto Chemicals)")


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
THIRTY_DAYS_AGO = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
TODAY_STR = datetime.now().strftime("%Y-%m-%d")


# === 7. Fetch Sales (Net, No Discount) ===
def get_sales(zid, start_date):
    """Fetch FZ items sold in last 30 days."""
    query = text("""
        SELECT
            opodt.xitem,
            caitem.xdesc,
            SUM(opodt.xqtyord) AS total_qty_sold,
            SUM(opodt.xlineamt) AS net_sales_amt
        FROM opord
        JOIN opodt ON opord.xordernum = opodt.xordernum
        JOIN caitem ON opodt.xitem = caitem.xitem
        WHERE opord.zid = :zid
          AND opodt.zid = :zid
          AND caitem.zid = :zid
          AND opord.xdate >= :start_date
          AND opodt.xitem LIKE '%%FZ%%'
        GROUP BY opodt.xitem, caitem.xdesc
        HAVING SUM(opodt.xlineamt) > 0
        ORDER BY opodt.xitem
    """)
    df = pd.read_sql(query, engine, params={'zid': zid, 'start_date': start_date})
    if not df.empty:
        df['avg_sale_price'] = (df['net_sales_amt'] / df['total_qty_sold']).round(2)
    return df


# === 8. Fetch MO Cost (Last 30 Days) ===
def get_mo_details(zid, start_date):
    """Get MO cost for items produced in last 30 days."""
    query = text("""
        SELECT
            moord.xitem,
            SUM((moodt.xqty * moodt.xrate) / NULLIF(moord.xqtyprd, 0)) AS mo_cost,
            moord.xqtyprd AS mo_qty
        FROM moord
        JOIN moodt ON moord.xmoord = moodt.xmoord
        WHERE moord.zid = :zid
          AND moodt.zid = :zid
          AND moord.xdatemo >= :start_date
        GROUP BY moord.xitem, moord.xmoord, moord.xqtyprd
        ORDER BY moord.xitem
    """)
    df = pd.read_sql(query, engine, params={'zid': zid, 'start_date': start_date})
    if not df.empty:
        df['mo_cost'] = df['mo_cost'].round(2)
    return df


# === 9. Fetch Last MO Cost (For Items Not Made Recently) ===
def get_last_mo_cost(zid, items):
    """Get latest MO cost for items not produced in last 30 days."""
    if not items:
        return pd.DataFrame(columns=['xitem', 'mo_cost', 'mo_qty'])
    query = text("""
        SELECT
            moord.xitem,
            SUM((moodt.xqty * moodt.xrate) / NULLIF(moord.xqtyprd, 0)) AS mo_cost,
            moord.xqtyprd AS mo_qty
        FROM moord
        JOIN moodt ON moord.xmoord = moodt.xmoord
        WHERE moord.zid = :zid
          AND moord.xitem IN :items
        GROUP BY moord.xitem, moord.xmoord, moord.xqtyprd
        ORDER BY moord.xitem DESC
    """)
    df = pd.read_sql(query, engine, params={'zid': zid, 'items': tuple(items)})
    if not df.empty:
        df = df.drop_duplicates(subset='xitem', keep='first')
        df['mo_cost'] = df['mo_cost'].round(2)
    return df[['xitem', 'mo_cost', 'mo_qty']] if not df.empty else pd.DataFrame(columns=['xitem', 'mo_cost', 'mo_qty'])


# === 10. Fetch GRN Cost (For Trading Items) ===
def get_grn_cost(zid, items, start_date):
    """Get average purchase cost from GRN for trading items."""
    if not items:
        return pd.DataFrame(columns=['xitem', 'mo_cost', 'mo_qty'])
    query = text("""
        SELECT
            xitem,
            SUM(xval) / NULLIF(SUM(xqty), 0) AS mo_cost,
            SUM(xqty) AS mo_qty
        FROM imtrn
        WHERE zid = :zid
          AND xitem IN :items
          AND xdate >= :start_date
          AND xdocnum LIKE '%%GRN%%'
        GROUP BY xitem
    """)
    df = pd.read_sql(query, engine, params={'zid': zid, 'items': tuple(items), 'start_date': start_date})
    if not df.empty:
        df['mo_cost'] = df['mo_cost'].round(2)
    return df


# === 11. Data Pipeline ===
print("📊 Fetching sales data...")
df_sales = get_sales(ZID, THIRTY_DAYS_AGO)
if df_sales.empty:
    raise ValueError("❌ No sales data found for FZ items.")

print("📊 Fetching MO cost data...")
df_mo = get_mo_details(ZID, THIRTY_DAYS_AGO)
df_mo_cost = df_mo.groupby('xitem').agg({'mo_cost': 'mean', 'mo_qty': 'sum'}).reset_index().round(2)

# Merge sales with MO cost
df_margin = pd.merge(df_sales, df_mo_cost, on='xitem', how='left').fillna(0)

# Split: with and without recent MO
df_with_mo = df_margin[df_margin['mo_cost'] != 0].copy()
df_without_mo = df_margin[df_margin['mo_cost'] == 0].copy()

# Drop mo_cost/mo_qty only if they exist (safe)
df_without_mo = df_without_mo.drop(columns=['mo_cost', 'mo_qty'], errors='ignore')

# Get last MO cost for items without recent production
items_without_mo = df_without_mo['xitem'].tolist()
df_last_mo = get_last_mo_cost(ZID, items_without_mo)
df_with_last_mo = pd.merge(df_without_mo, df_last_mo, on='xitem', how='left')

# Separate: non-trading (has MO) vs trading (no MO)
df_non_trading = df_with_last_mo.dropna(subset=['mo_cost']).copy()
df_trading = df_with_last_mo[df_with_last_mo['mo_cost'].isna()].copy()

# Known trading items
# === 11.5 Known trading items & GRN cost ===
TRADING_ITEMS = ("FZ000023", "FZ000024", "FZ000179")
df_trading_cost = get_grn_cost(ZID, TRADING_ITEMS, THIRTY_DAYS_AGO)

# Ensure GRN cost has required columns
for col in ['mo_cost', 'mo_qty']:
    if col not in df_trading_cost.columns:
        df_trading_cost[col] = 0.0

# Select only needed columns
df_trading_cost = df_trading_cost[['xitem', 'mo_cost', 'mo_qty']]

# Merge with trading items
df_trading_final = pd.merge(df_trading, df_trading_cost, on='xitem', how='left')

# Guarantee mo_cost and mo_qty exist
for col in ['mo_cost', 'mo_qty']:
    if col not in df_trading_final.columns:
        df_trading_final[col] = 0.0

# Safe fill
df_trading_final['mo_cost'] = df_trading_final['mo_cost'].fillna(0.0)
df_trading_final['mo_qty'] = df_trading_final['mo_qty'].fillna(0.0)


# === 12. Margin Calculation (Safe) ===
def calculate_margins(df):
    df = df.copy()
    for col in ['total_qty_sold', 'mo_cost', 'net_sales_amt']:
        if col not in df.columns:
            df[col] = 0.0
    df['total_cogs'] = (df['total_qty_sold'] * df['mo_cost']).round(2)
    df['gross_margin'] = (df['net_sales_amt'] - df['total_cogs']).round(2)
    return df

# Apply to all groups
df_with_mo = calculate_margins(df_with_mo)
df_non_trading = calculate_margins(df_non_trading)
df_trading_final = calculate_margins(df_trading_final)


# === 12. Margin Calculation (Safe) ===
def calculate_margins(df):
    """Safely calculate margins even if mo_cost is missing."""
    df = df.copy()
    # Ensure required columns exist
    for col in ['mo_cost', 'total_qty_sold', 'net_sales_amt']:
        if col not in df.columns:
            df[col] = 0.0
    df['total_cogs'] = (df['total_qty_sold'] * df['mo_cost']).round(2)
    df['gross_margin'] = (df['net_sales_amt'] - df['total_cogs']).round(2)
    return df

df_with_mo = calculate_margins(df_with_mo)
df_non_trading = calculate_margins(df_non_trading)
df_trading_final = calculate_margins(df_trading_final)


# === 13. Combine All & Add Total Row ===
df_all = pd.concat([df_with_mo, df_non_trading, df_trading_final], ignore_index=True)
df_all = calculate_margins(df_all)

def add_total_row(df):
    numeric_cols = df.select_dtypes(include='number').columns
    total = df[numeric_cols].sum().to_frame().T
    total['xitem'] = 'Total'
    total['xdesc'] = 'Total'
    for col in df.columns:
        if col not in total.columns:
            total[col] = None
    total = total[df.columns]
    return pd.concat([df, total], ignore_index=True)

df_all_with_total = add_total_row(df_all)
df_all_with_total['avg_costing_%'] = 0.0
mask = df_all_with_total['xitem'] != 'Total'
df_all_with_total.loc[mask, 'avg_costing_%'] = (
    (df_all_with_total.loc[mask, 'mo_cost'] * 100) / df_all_with_total.loc[mask, 'avg_sale_price']
).round(2)

# Sort: highest margin first, total at end
data_rows = df_all_with_total[df_all_with_total['xitem'] != 'Total'].sort_values('gross_margin', ascending=False)
final_df = pd.concat([data_rows, df_all_with_total[df_all_with_total['xitem'] == 'Total']], ignore_index=True)
final_df = final_df.round(2)


# === 14. Export to Excel ===
OUTPUT_FILE = f"H_10_Zepto_Margin_Cost.xlsx"
final_df.to_excel(OUTPUT_FILE, index=False, engine='openpyxl')
print(f"✅ Exported to {OUTPUT_FILE}")


# === 15. Send Email ===
try:
    recipients = get_email_recipients(os.path.splitext(os.path.basename(__file__))[0])
    print(f"📬 Recipients: {recipients}")
except Exception as e:
    print(f"⚠️ Failed to get recipients: {e}. Using fallback.")
    recipients = ["ithmbrbd@gmail.com"]

subject = f"H_10 Zepto Margin Cost Report – {TODAY_STR}"
body_text = "Please find the 30-day margin analysis for Zepto FZ items."

# Prepare HTML tables with subtotal
def prepare_email_table(df):
    return add_total_row(df.sort_values('gross_margin', ascending=False)) if not df.empty else df

html_df_list = [
    (prepare_email_table(df_with_mo), "Items with recent production"),
    (prepare_email_table(df_non_trading), "Items using last MO cost"),
    (prepare_email_table(df_trading_final), "Trading items (GRN cost)"),
]

send_mail(
    subject=subject,
    bodyText=body_text,
    attachment=[OUTPUT_FILE],
    recipient=recipients,
    html_body=html_df_list
)


# === 16. Cleanup ===
engine.dispose()
print("✅ H_10 completed successfully.")