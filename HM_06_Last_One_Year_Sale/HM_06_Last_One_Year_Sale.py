"""
🚀 HM_06_Last_One_Year_Sale.py – Last Year Sales, Returns & Inventory Std Price

📌 PURPOSE:
    - Sales from start of current year
    - Returns in same period
    - Inventory stock & average std price by month
    - Export to Excel: 3 sheets (sale, return, inv_stdprice)
    - Move file to all_sales/ folder

🔧 DATA SOURCES:
    - Sales: opdor, opddt
    - Returns: opcrn, opcdt
    - Inventory: imtrn, caitem
    - Database: PostgreSQL via DATABASE_URL

🏢 INPUT:
    - ZID from .env (e.g., ZID_GULSHAN_TRADING=100001)

📧 EMAIL:
    - Recipients: get_email_recipients("HM_06_Last_One_Year_Sale")
    - Fallback: ithmbrbd@gmail.com

📁 OUTPUT:
    - 64_66_one_year_sale_return_stdPrice_YYYY-01-01.xlsx
    - Moved to all_sales/ folder
"""

import os
import sys
import pandas as pd
from datetime import datetime
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

print(f"📌 Processing for ZID={ZID}")


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
CURRENT_YEAR_START = datetime.now().strftime("%Y-01-01")
TODAY = datetime.now().strftime("%Y-%m-%d")
OUTPUT_FILE = f"HM_06_{CURRENT_YEAR_START}_Sale.xlsx"
OUTPUT_FOLDER = "all_sales"


# === 7. Fetch Sales Data ===
def get_sales(zid, start_date):
    """Fetch sales data from start of year."""
    query = """
        SELECT
            opdor.xdornum, opdor.xdate, opdor.xsp, prmst.xname, opdor.xdiv, opdor.xcus, cacus.xshort,
            opddt.xitem, caitem.xdesc, caitem.xabc,
            SUM(opddt.xqty) AS qty,
            SUM(opddt.xlineamt) AS total_amount
        FROM opdor
        LEFT JOIN opddt ON opdor.xdornum = opddt.xdornum
        LEFT JOIN prmst ON opdor.xsp = prmst.xemp
        LEFT JOIN cacus ON opdor.xcus = cacus.xcus
        LEFT JOIN caitem ON opddt.xitem = caitem.xitem
        WHERE opdor.zid = %(zid)s
          AND opddt.zid = %(zid)s
          AND prmst.zid = %(zid)s
          AND cacus.zid = %(zid)s
          AND caitem.zid = %(zid)s
          AND opdor.xdate >= %(start_date)s
        GROUP BY opdor.xdornum, opdor.xdate, opdor.xsp, prmst.xname, opdor.xdiv, opdor.xcus,
                 cacus.xshort, opddt.xitem, caitem.xdesc, caitem.xabc
        ORDER BY opdor.xdornum
    """
    return pd.read_sql(query, engine, params={'zid': zid, 'start_date': start_date})


print("📊 Fetching sales data...")
df_sales = get_sales(ZID, CURRENT_YEAR_START)
if df_sales.empty:
    raise ValueError("❌ No sales data found.")

df_sales['xdate'] = pd.to_datetime(df_sales['xdate'])
df_sales['Year'] = df_sales['xdate'].dt.year
df_sales['Month'] = df_sales['xdate'].dt.strftime('%B')

df_sales = df_sales[[
    'xdornum', 'xdate', 'Year', 'Month', 'xsp', 'xname', 'xdiv', 'xcus', 'xshort',
    'xitem', 'xdesc', 'xabc', 'qty', 'total_amount'
]].rename(columns={
    'xdornum': 'OrderNumber',
    'xdate': 'Date',
    'xsp': 'SP_ID',
    'xname': 'SP_Name',
    'xdiv': 'Area',
    'xcus': 'CustomerID',
    'xshort': 'CustomerName',
    'xitem': 'ProductCode',
    'xdesc': 'ProductName',
    'xabc': 'ProductGroup',
    'qty': 'SalesQty',
    'total_amount': 'TotalSalesAmt'
})
df_sales['Date'] = df_sales['Date'].astype(str)


# === 8. Fetch Return Data ===
def get_returns(zid, start_date, end_date):
    """Fetch return data from start of year."""
    query = """
        SELECT
            opcrn.xcrnnum, opcrn.xdate, opcrn.xcus, cacus.xshort, cacus.xcity,
            opcrn.xemp, prmst.xname, opcdt.xitem, caitem.xdesc, caitem.xabc,
            SUM(opcdt.xqty) AS ret_qty,
            SUM(opcdt.xlineamt) AS ret_total
        FROM opcrn
        JOIN opcdt ON opcrn.xcrnnum = opcdt.xcrnnum
        JOIN prmst ON opcrn.xemp = prmst.xemp
        JOIN caitem ON opcdt.xitem = caitem.xitem
        JOIN cacus ON opcrn.xcus = cacus.xcus
        WHERE opcrn.zid = %(zid)s
          AND opcdt.zid = %(zid)s
          AND prmst.zid = %(zid)s
          AND caitem.zid = %(zid)s
          AND cacus.zid = %(zid)s
          AND opcrn.xdate >= %(start_date)s
          AND opcrn.xdate <= %(end_date)s
        GROUP BY opcrn.xcrnnum, opcrn.xdate, opcrn.xcus, cacus.xshort, cacus.xcity,
                 opcrn.xemp, prmst.xname, opcdt.xitem, caitem.xdesc, caitem.xabc
    """
    return pd.read_sql(query, engine, params={'zid': zid, 'start_date': start_date, 'end_date': end_date})


print("📊 Fetching return data...")
df_returns = get_returns(ZID, CURRENT_YEAR_START, TODAY)

# Convert date and extract year/month
df_returns['xdate'] = pd.to_datetime(df_returns['xdate'], errors='coerce')
df_returns = df_returns.dropna(subset=['xdate'])
df_returns['Year'] = df_returns['xdate'].dt.year
df_returns['Month'] = df_returns['xdate'].dt.month_name()

# ✅ Critical: Rename xitem to ProductCode
df_returns = df_returns.rename(columns={'xitem': 'ProductCode'})


# === 9. Merge Sales & Returns ===
df_sales_grouped = df_sales.groupby('ProductCode').agg({
    'SalesQty': 'sum',
    'TotalSalesAmt': 'sum'
}).reset_index()

df_returns_grouped = df_returns.groupby('ProductCode').agg({
    'ret_qty': 'sum',
    'ret_total': 'sum'
}).reset_index().rename(columns={'ret_qty': 'ReturnQty', 'ret_total': 'TotalReturn'})

df_sales_return = pd.merge(df_sales_grouped, df_returns_grouped, on='ProductCode', how='left').fillna(0)

df_sales_return = df_sales_return[[
    'ProductCode', 'SalesQty', 'TotalSalesAmt', 'ReturnQty', 'TotalReturn'
]]


# === 10. Inventory: Stock & Std Price by Month ===def get_inventory_data(zid, end_date):
def get_inventory_data(zid, end_date):
    """Fetch monthly inventory stock and average std price."""
    # Stock from imtrn
    stock_query = """
        SELECT
            imtrn.xitem,
            caitem.xdesc,
            caitem.xabc AS ProductGroup,
            imtrn.xyear AS year,
            imtrn.xper AS month,
            SUM(imtrn.xqty * imtrn.xsign) AS stock,  -- ← lowercase: 'stock'
            caitem.xstdprice AS caitemStdPrice
        FROM imtrn
        JOIN caitem ON imtrn.xitem = caitem.xitem
        WHERE imtrn.zid = %(zid)s
          AND imtrn.xdate <= %(end_date)s
          AND imtrn.xwh = 'HMBR -Main Store (4th Floor)'
        GROUP BY imtrn.xitem, caitem.xdesc, imtrn.xyear, imtrn.xper, caitem.xstdprice, caitem.xabc
        ORDER BY imtrn.xitem
    """
    df_stock = pd.read_sql(stock_query, engine, params={'zid': zid, 'end_date': end_date})

    # Average std price from sales
    price_query = """
        SELECT
            opddt.xitem,
            EXTRACT(YEAR FROM opdor.xdate)::int AS year,
            EXTRACT(MONTH FROM opdor.xdate)::int AS month,
            SUM(opddt.xlineamt) / SUM(opddt.xqty) AS stdprice
        FROM opddt
        JOIN opdor ON opddt.xdornum = opdor.xvoucher
        WHERE opdor.zid = %(zid)s
          AND opddt.zid = %(zid)s
          AND opdor.xdate <= %(end_date)s
          AND opddt.xlineamt > 0
        GROUP BY opddt.xitem, year, month
    """
    df_price = pd.read_sql(price_query, engine, params={'zid': zid, 'end_date': end_date})

    # Merge stock and price
    df = pd.merge(df_stock, df_price, on=['xitem', 'year', 'month'], how='left')

    # ✅ Use lowercase 'stock' consistently
    df['stock'] = df.groupby('xitem')['stock'].cumsum()  # ← now works
    df['stdprice'] = df.groupby('xitem')['stdprice'].fillna(method='ffill')

    # Date formatting
    df['Date'] = pd.to_datetime(df[['year', 'month']].assign(day=1)) + pd.offsets.MonthEnd(0)
    df['Date'] = df['Date'].astype(str)
    df['Month'] = pd.to_datetime(df['month'], format='%m').dt.month_name()
    df['Year'] = df['year'].astype(str)

    df = df.rename(columns={
        'xitem': 'ProductCode',
        'xdesc': 'ProductName',
        'productgroup': 'ProductGroup',
        'stdprice': 'StdPrice'
    })

    # Use lowercase 'stock' in final calc
    df['StockAmount'] = df['stock'] * df['StdPrice']

    return df[[
        'ProductCode', 'ProductName', 'ProductGroup', 'Year', 'Month',
        'stock', 'StdPrice', 'StockAmount', 'Date'
    ]].rename(columns={'stock': 'Stock'})  # ← Only rename to 'Stock' at the end

print("📊 Fetching inventory data...")
df_inventory = get_inventory_data(ZID, TODAY)

# Filter last (current) year
last_year = datetime.now().year
df_inventory_current = df_inventory[df_inventory['Year'] == str(last_year)]


# === 11. Export to Excel ===
print(f"📁 Writing to {OUTPUT_FILE}...")
with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
    df_sales.to_excel(writer, sheet_name='oneyear_sale', index=False)
    df_sales_return.to_excel(writer, sheet_name='return', index=False)
    df_inventory_current.to_excel(writer, sheet_name='inv_stdprice', index=False)

print("✅ Excel file created.")


# === 12. Move File to all_sales Folder ===
if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

moved_file = os.path.join(OUTPUT_FOLDER, os.path.basename(OUTPUT_FILE))
try:
    os.replace(OUTPUT_FILE, moved_file)  # Safe move (overwrite if exists)
    print(f"📁 File moved to: {moved_file}")
except Exception as e:
    print(f"❌ Failed to move file: {e}")


# === 13. Send Email ===
try:
    recipients = get_email_recipients("HM_06_Last_One_Year_Sale")
    # recipients = ["ithmbrbd@gmail.com"]
    print(f"📬 Recipients: {recipients}")
except Exception as e:
    print(f"⚠️ Fallback: {e}")
    recipients = ["ithmbrbd@gmail.com"]

subject = f"HM_06 Last Year Sales & Inventory Std Price ({last_year})"
body_text = (
    f"Sales, returns, and inventory standard price report from {CURRENT_YEAR_START} to {TODAY}.\n"
    "Includes: oneyear_sale, return, inv_stdprice sheets."
)

send_mail(
    subject=subject,
    bodyText=body_text,
    attachment=[moved_file],
    recipient=recipients,
    html_body=None
)


# === 14. Cleanup ===
engine.dispose()
print("✅ HM_06 completed successfully.")