"""
📊 HM_16_Item_Wise_Profit.py – 30-Day Item-Wise Profit & Loss Report

🚀 PURPOSE:
    - Calculate item-wise gross & net profit for 6 businesses
    - Uses COGS (inventory), returns (SR--), and GL expenses
    - Export to one Excel (multi-sheet) + HTML email summary

🏢 BUSINESSES:
    - GI (ZID_GI)
    - HMBR (Trading)
    - Zepto Chemicals
    - HMBR Grocery Shop
    - HMBR Online Shop (Paint Roller)
    - Gulshan Packaging

📁 OUTPUT:
    - HM_16_Item_Wise_Profit.xlsx (one sheet per business)
    - Email with vertical HTML summary (SL No.)

📬 EMAIL:
    - Recipients: get_email_recipients("HM_16_Item_Wise_Profit")
    - Fallback: ithmbrbd@gmail.com, asaddat87@gmail.com

📅 PERIOD: Last 31 days (auto-calculated)
"""

import os
import sys
import pandas as pd
import numpy as np
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
    ZID_GI = int(os.environ["ZID_GI"])                    # 100000 → GI
    ZID_HMBR = int(os.environ["ZID_GULSHAN_TRADING"])     # 100001 → HMBR
    ZID_ZEPTO = int(os.environ["ZID_ZEPTO_CHEMICALS"])    # 100005 → Zepto
    ZID_GROCERY = int(os.environ["ZID_HMBR_GROCERY"])      # 100006 → Grocery
    ZID_PAINT_ROLLER = int(os.environ["ZID_HMBR_ONLINE_SHOP"])  # 100007 → HMBR Online Shop
    ZID_PACKAGING = int(os.environ["ZID_GULSHAN_PACKAGING"])    # 100009 → Packaging
except KeyError as e:
    raise RuntimeError(f"❌ Missing ZID in .env: {e}")

# ZID → Project Name Mapping (from .env)
PROJECT_MAP = {}
for key, value in os.environ.items():
    if key.startswith("PROJECT_") and value.strip():
        try:
            zid = int(key.split("PROJECT_")[1])
            PROJECT_MAP[zid] = value.strip()
        except ValueError:
            continue

# COGS & MRP Accounts
COGS_ACCOUNT = "04010020"
MRP_ACCOUNT_ZEPTO = "07080001"

# Date Range: Last 31 days
end_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
start_date = (datetime.now() - timedelta(days=33)).strftime('%Y-%m-%d')

print(f"📅 Reporting Period: {start_date} → {end_date}")


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
# 📥 4. Fetch Sales & COGS (DO--)
# ─────────────────────────────────────────────────────────────────────
def get_sales_cogs(zid, start_date, end_date):
    query = text("""
        SELECT 
            caitem.xitem, caitem.xdesc,
            SUM(imtrn.xval * imtrn.xsign) AS totalvalue,
            SUM(opddt.xlineamt) AS xlineamt,
            SUM(opddt.xdtwotax - opddt.xdtdisc) AS xdtwotax
        FROM caitem
        JOIN imtrn ON caitem.xitem = imtrn.xitem
        JOIN opddt ON imtrn.xdocnum = opddt.xdornum 
                   AND imtrn.xitem = opddt.xitem 
                   AND imtrn.xdocrow = opddt.xrow
        JOIN opdor ON imtrn.xdocnum = opdor.xdornum
        WHERE caitem.zid = :zid
          AND imtrn.zid = :zid
          AND opddt.zid = :zid
          AND opdor.zid = :zid
          AND imtrn.xdoctype = 'DO--'
          AND imtrn.xdate >= :start_date
          AND imtrn.xdate <= :end_date
        GROUP BY caitem.xitem, caitem.xdesc
    """)
    return pd.read_sql(query, engine, params={'zid': zid, 'start_date': start_date, 'end_date': end_date})


# ─────────────────────────────────────────────────────────────────────
# 📤 5. Fetch Returns (SR--)
# ─────────────────────────────────────────────────────────────────────
def get_returns(zid, start_date, end_date):
    query = text("""
        SELECT 
            imtrn.xitem,
            SUM(imtrn.xval * imtrn.xsign) AS returnvalue,
            SUM(opcdt.xrate * imtrn.xqty) AS totamt
        FROM imtrn
        JOIN opcdt ON imtrn.xdocnum = opcdt.xcrnnum AND imtrn.xitem = opcdt.xitem
        JOIN opcrn ON imtrn.xdocnum = opcrn.xcrnnum
        WHERE imtrn.zid = :zid
          AND opcdt.zid = :zid
          AND opcrn.zid = :zid
          AND imtrn.xdoctype = 'SR--'
          AND imtrn.xdate >= :start_date
          AND imtrn.xdate <= :end_date
        GROUP BY imtrn.xitem
    """)
    return pd.read_sql(query, engine, params={'zid': zid, 'start_date': start_date, 'end_date': end_date})


# ─────────────────────────────────────────────────────────────────────
# 💰 6. Fetch GL Income & Expenses
# ─────────────────────────────────────────────────────────────────────
def get_gl_details(zid, start_date, end_date, project_filter=None):
    if project_filter:
        # Use project filter (GI, HMBR, etc.)
        query = text("""
            SELECT glmst.xacctype, SUM(gldetail.xprime) AS amount
            FROM glmst
            JOIN gldetail ON glmst.xacc = gldetail.xacc
            JOIN glheader ON gldetail.xvoucher = glheader.xvoucher
            WHERE glmst.zid = :zid
              AND gldetail.zid = :zid
              AND glheader.zid = :zid
              AND gldetail.xproj = :project
              AND glmst.xacctype IN ('Income', 'Expenditure')
              AND glmst.xacc != :cogs_acc
              AND glheader.xdate >= :start_date
              AND glheader.xdate <= :end_date
            GROUP BY glmst.xacctype
        """)
        return pd.read_sql(query, engine, params={
            'zid': zid, 'project': project_filter, 'cogs_acc': COGS_ACCOUNT,
            'start_date': start_date, 'end_date': end_date
        })
    else:
        # No project filter (Zepto: exclude MRP)
        query = text("""
            SELECT glmst.xacctype, SUM(gldetail.xprime) AS amount
            FROM glmst
            JOIN gldetail ON glmst.xacc = gldetail.xacc
            JOIN glheader ON gldetail.xvoucher = glheader.xvoucher
            WHERE glmst.zid = :zid
              AND gldetail.zid = :zid
              AND glheader.zid = :zid
              AND glmst.xacctype IN ('Income', 'Expenditure')
              AND glmst.xacc != :cogs_acc
              AND glmst.xacc != :mrp_acc
              AND glheader.xdate >= :start_date
              AND glheader.xdate <= :end_date
            GROUP BY glmst.xacctype
        """)
        return pd.read_sql(query, engine, params={
            'zid': zid, 'cogs_acc': COGS_ACCOUNT, 'mrp_acc': MRP_ACCOUNT_ZEPTO,
            'start_date': start_date, 'end_date': end_date
        })


# ─────────────────────────────────────────────────────────────────────
# 🧮 7. Process Selected Businesses
# ─────────────────────────────────────────────────────────────────────
businesses = [
    {"zid": ZID_GI,           "name": "GI",                  "use_project": True},
    {"zid": ZID_HMBR,         "name": "HMBR",                "use_project": True},
    {"zid": ZID_ZEPTO,        "name": "Zepto",               "use_project": False},
    {"zid": ZID_GROCERY,      "name": "HMBR Grocery Shop",   "use_project": True},
    {"zid": ZID_PAINT_ROLLER, "name": "HMBR Online Shop",    "use_project": True},
    {"zid": ZID_PACKAGING,    "name": "Gulshan Packaging",   "use_project": True},
]

main_data_dict = {}
all_reports = {}

for biz in businesses:
    zid = biz["zid"]
    name = biz["name"]
    print(f"📊 Processing {name} (ZID={zid})...")

    # Get project name from .env
    project_name = PROJECT_MAP.get(zid)

    # Fetch sales
    df_sales = get_sales_cogs(zid, start_date, end_date)
    if df_sales.empty:
        print(f"❌ No sales data for {name}")
        continue

    # Ensure required columns exist
    for col in ['totalvalue', 'xlineamt', 'xdtwotax']:
        if col not in df_sales.columns:
            df_sales[col] = 0.0
    df_sales = df_sales.groupby(['xitem', 'xdesc'])[['totalvalue', 'xlineamt', 'xdtwotax']].sum().round(1).reset_index()

    # Fetch returns
    df_return = get_returns(zid, start_date, end_date)
    if df_return.empty:
        df_return = pd.DataFrame(columns=['xitem', 'returnvalue', 'totamt'])
    else:
        for col in ['returnvalue', 'totamt']:
            if col not in df_return.columns:
                df_return[col] = 0.0
        df_return = df_return.groupby(['xitem'])[['returnvalue', 'totamt']].sum().round(1).reset_index()

    # Merge with sales
    df_final = df_sales.merge(
        df_return[['xitem', 'returnvalue', 'totamt']],
        on='xitem',
        how='left'
    ).fillna(0)

    # Determine sales column
    if name == "Zepto":
        sales_col = 'xdtwotax' if 'xdtwotax' in df_final.columns else 'xlineamt'
    else:
        sales_col = 'xlineamt' if 'xlineamt' in df_final.columns else 'xdtwotax'

    if sales_col not in df_final.columns:
        df_final[sales_col] = 0.0

    # Final calculations
    df_final['final_sales'] = df_final[sales_col] - df_final['totamt']
    df_final['final_cost'] = df_final['totalvalue']
    if biz["use_project"]:
        df_final['final_cost'] += df_final['returnvalue']

    df_final['Gross_Profit'] = df_final['final_sales'] + df_final['final_cost']
    df_final['Profit_Ratio'] = (df_final['Gross_Profit'] / df_final['final_cost']) * -100
    df_final = df_final.replace([np.inf, -np.inf], 0)

    # Sort and add total
    df_final = df_final.sort_values('Profit_Ratio', ascending=False).reset_index(drop=True)
    total_row = df_final.sum(numeric_only=True)
    total_row['xdesc'] = 'Total_Item_Profit'
    total_row['xitem'] = 'TOTAL'
    df_final = pd.concat([df_final, pd.DataFrame([total_row])], ignore_index=True)

    # Fetch GL data
    if biz["use_project"] and project_name:
        df_gl = get_gl_details(zid, start_date, end_date, project_filter=project_name)
    else:
        df_gl = get_gl_details(zid, start_date, end_date)

    gl_dict = df_gl.set_index('xacctype')['amount'].to_dict() if not df_gl.empty else {}
    income_gl = gl_dict.get('Income', 0)
    expense_gl = gl_dict.get('Expenditure', 0)

    # Summary
    last_row = df_final.iloc[-1]
    summary = {
        'final_sales': last_row['final_sales'],
        'final_cost': last_row['final_cost'],
        'Gross_Profit': last_row['Gross_Profit'],
        'Profit_Ratio': last_row['Profit_Ratio'],
        'Income_gl': income_gl,
        'Expenditure_gl': expense_gl,
        'Net': last_row['Gross_Profit'] - expense_gl
    }

    main_data_dict[name] = summary
    all_reports[name] = df_final


# ─────────────────────────────────────────────────────────────────────
# 📊 8. Prepare HTML Summary (Vertical, SL No.)
# ─────────────────────────────────────────────────────────────────────
print("📈 Preparing HTML summary...")

df_summary = pd.DataFrame(main_data_dict).T.reset_index().rename(columns={'index': 'Business'})
df_summary = df_summary.round(2)
df_summary.insert(0, 'SL', range(1, len(df_summary) + 1))
cols = ['SL', 'Business'] + [c for c in ['final_sales', 'final_cost', 'Gross_Profit', 'Profit_Ratio', 'Net'] if c in df_summary.columns]
df_summary = df_summary[cols]


# ─────────────────────────────────────────────────────────────────────
# 📁 9. Export to Excel
# ─────────────────────────────────────────────────────────────────────
OUTPUT_FILE = "HM_16_Item_Wise_Profit.xlsx"

print(f"📁 Writing to {OUTPUT_FILE}...")
with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
    for name, df in all_reports.items():
        # Clean sheet name (max 31 chars, no special chars)
        sheet_name = name.replace(" ", "_")[:31]
        df.to_excel(writer, sheet_name=sheet_name, index=False)

# Auto column width
from openpyxl import load_workbook
wb = load_workbook(OUTPUT_FILE)
for sheet in wb:
    sheet.column_dimensions['C'].width = 45  # xdesc
    for col in sheet.columns:
        max_length = min(max(len(str(cell.value)) for cell in col) + 2, 50)
        sheet.column_dimensions[col[0].column_letter].width = max_length
wb.save(OUTPUT_FILE)

print(f"✅ Excel saved: {OUTPUT_FILE}")


# ─────────────────────────────────────────────────────────────────────
# 📬 10. Send Email
# ─────────────────────────────────────────────────────────────────────
try:
    recipients = get_email_recipients(os.path.splitext(os.path.basename(__file__))[0])
    print(f"📬 Recipients: {recipients}")
except Exception as e:
    print(f"⚠️ Fallback: {e}")
    recipients = ["ithmbrbd@gmail.com", "asaddat87@gmail.com"]

subject = f"HM_16 Item-Wise Profit Report – {start_date} to {end_date}"
body_text = f"""
Dear Sir,

Please find the 30-day item-wise profit & loss report for 6 businesses.

Period: {start_date} to {end_date}

Highlights: \n
- Total Gross Profit: ৳{df_summary['Gross_Profit'].sum():,.2f} \n
- Total Net Profit: ৳{df_summary['Net'].sum():,.2f}\n

Full details in attachment. \n
"""

send_mail(
    subject=subject,
    bodyText=body_text,
    attachment=[OUTPUT_FILE],
    recipient=recipients,
    html_body=[(df_summary, "Profit Summary by Business")]
)

print("✅ HM_16 completed successfully.")