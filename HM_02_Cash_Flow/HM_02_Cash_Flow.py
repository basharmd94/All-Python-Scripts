"""
🚀 HM_02_Cash_Flow.py – Automated Financial Report Generator

📌 PURPOSE:
    Generates Cash Flow and P&L reports for HMBR companies.
    - hmbr_cf.xlsx: Cash Flow (one sheet per project: cf_ProjectName)
    - hmbr_pl.xlsx: P&L (one sheet per project: pl_ProjectName)

🔧 DATA SOURCES:
    - GL: glmst, gldetail, glheader
    - Masters: caitem, casup, cacus
    - Database: PostgreSQL via DATABASE_URL in project_config.py

🏢 COMPANIES:
    Dynamically loaded from .env as PROJECT_100000=Karigor Ltd., etc.

📧 EMAIL:
    Sends reports via email using shared modules.
    Recipients from get_email_recipients(__file__) or fallback to ithmbrbd@gmail.com

💡 NOTE:
    - Uses parameterized queries to prevent SQL injection.
    - Project names are loaded from .env → fully dynamic.
    - Sheet names reflect current project names (e.g., cf_GULSHAN TRADING).
"""

import sys
import os
import re
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from dotenv import load_dotenv  # <-- Added


# === 1. Load Environment Variables from .env ===
load_dotenv()

# Build project_zid and zid_to_name from environment
project_zid = {}   # name -> zid
zid_to_name = {}   # zid -> name

for key, value in os.environ.items():
    if key.startswith("PROJECT_"):
        try:
            zid = int(key.replace("PROJECT_", ""))
            name = value.strip()
            project_zid[name] = zid
            zid_to_name[zid] = name
        except ValueError:
            print(f"⚠️ Invalid ZID in .env: {key}={value}")

if not project_zid:
    raise RuntimeError("❌ No valid PROJECT_* entries found in .env file. Please check.")


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

# === 5. Suppress warnings safely ===
pd.options.mode.chained_assignment = None
import warnings
warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)
warnings.filterwarnings('ignore', category=FutureWarning, message=".*downcast.*")


# === 6. SQL Query Functions (Parameterized, Secure) ===
def get_cashflow_details(zid, project, start_date, end_date):
    """
    Fetch ALL transactions from vouchers that involve key cash/bank accounts.
    This includes non-cash entries (e.g., salary, loans) if they're on the same voucher.
    """
    query = """
        SELECT 
            glmst.zid, 
            glmst.xacc, 
            glheader.xper, 
            glheader.xyear, 
            SUM(gldetail.xprime) AS sum
        FROM glmst
        JOIN gldetail ON glmst.xacc = gldetail.xacc
        JOIN glheader ON gldetail.xvoucher = glheader.xvoucher
        WHERE glmst.zid = %(zid)s
          AND gldetail.zid = %(zid)s
          AND glheader.zid = %(zid)s
          AND gldetail.xproj = %(project)s
          AND glheader.xper != 0
          AND glheader.xdate >= %(start_date)s
          AND glheader.xdate <= %(end_date)s
          AND gldetail.xvoucher IN (
            SELECT DISTINCT gldetail.xvoucher
            FROM gldetail
            JOIN glheader ON glheader.xvoucher = gldetail.xvoucher
            JOIN glmst ON glmst.xacc = gldetail.xacc
            WHERE gldetail.zid = %(zid)s
              AND glheader.zid = %(zid)s
              AND glmst.zid = %(zid)s
              AND gldetail.xproj = %(project)s
              AND gldetail.xacc IN ('01020001', '01010001', '10010003', '10010006')
              AND glheader.xper != 0
              AND glheader.xdate >= %(start_date)s
              AND glheader.xdate <= %(end_date)s
          )
        GROUP BY glmst.zid, glmst.xacc, glheader.xper, glheader.xyear
    """
    return pd.read_sql(query, engine, params={
        'zid': zid,
        'project': project,
        'start_date': start_date,
        'end_date': end_date
    })


def get_gl_details_project(zid, project, start_date, end_date):
    """
    Fetch income and expenditure entries for P&L report.
    Excludes Asset and Liability accounts.
    """
    query = """
        SELECT 
            glmst.zid, 
            glmst.xacc, 
            glmst.xacctype, 
            glheader.xyear, 
            glheader.xper,
            SUM(gldetail.xprime) AS sum
        FROM glmst
        JOIN gldetail ON glmst.xacc = gldetail.xacc
        JOIN glheader ON gldetail.xvoucher = glheader.xvoucher
        WHERE glmst.zid = %(zid)s
          AND gldetail.zid = %(zid)s
          AND glheader.zid = %(zid)s
          AND gldetail.xproj = %(project)s
          AND (glmst.xacctype = 'Income' OR glmst.xacctype = 'Expenditure')
          AND glheader.xdate >= %(start_date)s
          AND glheader.xdate <= %(end_date)s
        GROUP BY glmst.zid, glmst.xacc, glheader.xyear, glheader.xper
        ORDER BY glheader.xper ASC, glmst.xacctype
    """
    return pd.read_sql(query, engine, params={
        'zid': zid,
        'project': project,
        'start_date': start_date,
        'end_date': end_date
    })


def get_gl_master(zid):
    """Fetch GL account master data: description, hierarchy, type."""
    query = "SELECT zid, xacc, xdesc, xhrc3, xacctype FROM glmst WHERE zid = %(zid)s"
    return pd.read_sql(query, engine, params={'zid': zid})


# === 7. Account Label Mapping ===
label_data = [
    {'xacc': '10020014', 'Label': 'Other Loan'},
    {'xacc': '10020012', 'Label': 'Internal Loan'},
    {'xacc': '10020008', 'Label': 'Internal Loan'},
    {'xacc': '10020011', 'Label': 'Sunflower Loan'},
    {'xacc': '10010001', 'Label': 'DBL - LTR'},
    {'xacc': '12010002', 'Label': 'DBL - Building Term Loan'},
    {'xacc': '10010004', 'Label': 'DBL - Other Term Loan'},
    {'xacc': '10020015', 'Label': 'MD Sir Loan'},
    {'xacc': '10010006', 'Label': 'UCB - CC'},
    {'xacc': '10020002', 'Label': 'Internal Loan'},
    {'xacc': '10020016', 'Label': 'Internal Loan'},
    {'xacc': '10020001', 'Label': 'MD Sir Loan'},
    {'xacc': '10010003', 'Label': 'DBL - CC'},
    {'xacc': '03080001', 'Label': 'Investment'},
    {'xacc': '02030002', 'Label': 'Investment'},
    {'xacc': '02050013', 'Label': 'Investment'},
    {'xacc': '02050011', 'Label': 'Investment'},
    {'xacc': '02050010', 'Label': 'Investment'},
    {'xacc': '02050007', 'Label': 'Investment'},
    {'xacc': '02050006', 'Label': 'Investment'},
    {'xacc': '02050004', 'Label': 'Investment'},
    {'xacc': '02050001', 'Label': 'Investment'},
    {'xacc': '02050016', 'Label': 'Investment'},
    {'xacc': '05010004', 'Label': 'Expense'},
    {'xacc': '01010001', 'Label': 'Cash Reserves '},
    {'xacc': '05010001', 'Label': 'Expense'},
    {'xacc': '04010018', 'Label': 'Expense'},
    {'xacc': '06030006', 'Label': 'Expense'},
    {'xacc': '06070002', 'Label': 'Expense'},
    {'xacc': '06010004', 'Label': 'Expense'},
    {'xacc': '06100001', 'Label': 'Expense'},
    {'xacc': '06070003', 'Label': 'Expense'},
    {'xacc': '06220009', 'Label': 'Expense'},
    {'xacc': '05010003', 'Label': 'Expense'},
    {'xacc': '06010001', 'Label': 'Expense'},
    {'xacc': '06060002', 'Label': 'Expense'},
    {'xacc': '06010005', 'Label': 'Expense'},
    {'xacc': '06230003', 'Label': 'Expense'},
    {'xacc': '06220008', 'Label': 'Expense'},
    {'xacc': '06220012', 'Label': 'Expense'},
    {'xacc': '06060001', 'Label': 'Expense'},
    {'xacc': '06040008', 'Label': 'Expense'},
    {'xacc': '06040007', 'Label': 'Expense'},
    {'xacc': '06040003', 'Label': 'Expense'},
    {'xacc': '06040001', 'Label': 'Expense'},
    {'xacc': '06230001', 'Label': 'Expense'},
    {'xacc': '06030014', 'Label': 'Expense'},
    {'xacc': '06030012', 'Label': 'Expense'},
    {'xacc': '06030011', 'Label': 'Expense'},
    {'xacc': '06030010', 'Label': 'Expense'},
    {'xacc': '06220007', 'Label': 'Expense'},
    {'xacc': '06030009', 'Label': 'Expense'},
    {'xacc': '06030004', 'Label': 'Expense'},
    {'xacc': '06020001', 'Label': 'Expense'},
    {'xacc': '06010006', 'Label': 'Expense'},
    {'xacc': '06030013', 'Label': 'Expense'},
    {'xacc': '06220006', 'Label': 'Expense'},
    {'xacc': '06160007', 'Label': 'Expense'},
    {'xacc': '06220002', 'Label': 'Expense'},
    {'xacc': '13010003', 'Label': 'Adjustment'},
    {'xacc': '09010002', 'Label': 'Salary'},
    {'xacc': '09030001', 'Label': 'Accounts Payable'},
    {'xacc': '09030002', 'Label': 'Adjustment'},
    {'xacc': '09050023', 'Label': 'Adjustment'},
    {'xacc': '09010006', 'Label': 'Adjustment'},
    {'xacc': '11040001', 'Label': 'Adjustment'},
    {'xacc': '11020002', 'Label': 'Adjustment'},
    {'xacc': '11010003', 'Label': 'Adjustment'},
    {'xacc': '11010001', 'Label': 'Adjustment'},
    {'xacc': '08050002', 'Label': 'Other Income '},
    {'xacc': '08030002', 'Label': 'Other Income '},
    {'xacc': '08030001', 'Label': 'Other Income '},
    {'xacc': '08050003', 'Label': 'Other Income '},
    {'xacc': '08050004', 'Label': 'Other Income '},
    {'xacc': '08050008', 'Label': 'Other Income '},
    {'xacc': '06190002', 'Label': 'Expense'},
    {'xacc': '06210001', 'Label': 'Expense'},
    {'xacc': '06190005', 'Label': 'Expense'},
    {'xacc': '06190004', 'Label': 'Expense'},
    {'xacc': '06190003', 'Label': 'Expense'},
    {'xacc': '06220004', 'Label': 'Expense'},
    {'xacc': '06190001', 'Label': 'Expense'},
    {'xacc': '06220005', 'Label': 'Expense'},
    {'xacc': '06180004', 'Label': 'Expense'},
    {'xacc': '06170001', 'Label': 'Expense'},
    {'xacc': '06130001', 'Label': 'Expense'},
    {'xacc': '06160004', 'Label': 'Expense'},
    {'xacc': '06160003', 'Label': 'Expense'},
    {'xacc': '06160002', 'Label': 'Expense'},
    {'xacc': '06160001', 'Label': 'Expense'},
    {'xacc': '06180001', 'Label': 'Expense'},
    {'xacc': '07060002', 'Label': 'Expense'},
    {'xacc': '07010005', 'Label': 'Expense'},
    {'xacc': '07020004', 'Label': 'Expense'},
    {'xacc': '03010001', 'Label': 'Expense for Asset'},
    {'xacc': '01010005', 'Label': 'Salary'},
    {'xacc': '01010007', 'Label': 'Bank Transfer'},
    {'xacc': '01020001', 'Label': 'Cash Reserves '},
    {'xacc': '01020002', 'Label': 'Bank Transfer'},
    {'xacc': '01030001', 'Label': 'Income '},
    {'xacc': '01050001', 'Label': 'Accounts Payable - Intl'},
    {'xacc': '03040007', 'Label': 'Expense for Asset'},
    {'xacc': '01050002', 'Label': 'Adjustment'},
    {'xacc': '01050004', 'Label': 'Expense for Asset'},
    {'xacc': '01040005', 'Label': 'Salary'},
    {'xacc': '01050007', 'Label': 'VAT Expense'},
    {'xacc': '06310001', 'Label': 'Expense'},
    {'xacc': '07010004', 'Label': 'Expense'},
    {'xacc': '06310005', 'Label': 'Expense'},
    {'xacc': '06310006', 'Label': 'Expense'},
    {'xacc': '01050003', 'Label': 'Salary'},
    {'xacc': '03010003', 'Label': 'Expense for Asset'},
    {'xacc': '03010006', 'Label': 'Expense for Asset'},
    {'xacc': '03060010', 'Label': 'Expense for Asset'},
    {'xacc': '03010011', 'Label': 'Expense for Asset'},
    {'xacc': '03010013', 'Label': 'Expense for Asset'},
    {'xacc': '03010018', 'Label': 'Expense for Asset'},
    {'xacc': '03010022', 'Label': 'Expense for Asset'},
    {'xacc': '03030001', 'Label': 'Expense for Asset'},
    {'xacc': '03030003', 'Label': 'Expense for Asset'},
    {'xacc': '03040004', 'Label': 'Expense for Asset'},
    {'xacc': '03040005', 'Label': 'Expense for Asset'},
    {'xacc': '03040006', 'Label': 'Expense for Asset'},
    {'xacc': '03040009', 'Label': 'Expense for Asset'},
    {'xacc': '03040012', 'Label': 'Expense for Asset'},
    {'xacc': '03040017', 'Label': 'Expense for Asset'},
    {'xacc': '03040019', 'Label': 'Expense for Asset'},
    {'xacc': '03040021', 'Label': 'Expense for Asset'},
    {'xacc': '03040026', 'Label': 'Expense for Asset'},
    {'xacc': '03050001', 'Label': 'Expense for Asset'},
    {'xacc': '03060002', 'Label': 'Expense for Asset'},
    {'xacc': '06340001', 'Label': 'Expense'},
    {'xacc': '06320001', 'Label': 'Expense'},
    {'xacc': '06310004', 'Label': 'Expense'},
    {'xacc': '06350001', 'Label': 'Expense'},
    {'xacc': '07120004', 'Label': 'Expense'},
    {'xacc': '06320002', 'Label': 'Expense'},
    {'xacc': '07120002', 'Label': 'Expense'},
    {'xacc': '07120001', 'Label': 'Expense'},
    {'xacc': '07110003', 'Label': 'Expense'},
    {'xacc': '07100001', 'Label': 'Expense'},
    {'xacc': '07060005', 'Label': 'Expense'},
    {'xacc': '06230004', 'Label': 'Expense'},
    {'xacc': '07010001', 'Label': 'Expense'},
    {'xacc': '07060001', 'Label': 'Expense'},
    {'xacc': '07050003', 'Label': 'Expense'},
    {'xacc': '07050002', 'Label': 'Expense'},
    {'xacc': '07050001', 'Label': 'Expense'},
    {'xacc': '07030003', 'Label': 'Expense'},
    {'xacc': '07030002', 'Label': 'Expense'},
    {'xacc': '07030001', 'Label': 'Expense'},
    {'xacc': '07060004', 'Label': 'Expense'},
    {'xacc': '07130001', 'Label': 'Expense'},
    {'xacc': '07120003', 'Label': 'Expense'},
    {'xacc': '07010003', 'Label': 'Expense'},
    {'xacc': '06300002', 'Label': 'Expense'},
    {'xacc': '06310007', 'Label': 'Expense'},
    {'xacc': '06300001', 'Label': 'Expense'},
    {'xacc': '06250001', 'Label': 'Expense'},
    {'xacc': '06290002', 'Label': 'Expense'},
    {'xacc': '07130003', 'Label': 'Expense'},
    {'xacc': '06290001', 'Label': 'Expense'},
    {'xacc': '06280001', 'Label': 'Expense'},
    {'xacc': '06270001', 'Label': 'Expense'},
    {'xacc': '13010005', 'Label': 'Adjustment'},
    {'xacc': '06260002', 'Label': 'Expense'},
    {'xacc': '06260001', 'Label': 'Expense'},
    {'xacc': '06250002', 'Label': 'Expense'},
    {'xacc': '06240002', 'Label': 'Expense'},
    {'xacc': '07120005', 'Label': 'Expense'},
    {'xacc': '06380001', 'Label': 'Expense'},
    {'xacc': '06290003', 'Label': 'Expense'},
    {'xacc': '06370001', 'Label': 'Expense'},
    {'xacc': '06260003', 'Label': 'Expense'},
    {'xacc': '09040001', 'Label': 'Accounts Payable - Intl'},
    {'xacc': '09040003', 'Label': 'Accounts Payable - Intl'},
    {'xacc': '09040004', 'Label': 'Accounts Payable - Intl'},
    {'xacc': '09040006', 'Label': 'Accounts Payable - Intl'},
    {'xacc': '09040007', 'Label': 'UCB - CC'},
    {'xacc': '09050019', 'Label': 'Accounts Payable - Intl'},
    {'xacc': '09040002', 'Label': 'Accounts Payable - Intl'},
    {'xacc': '09050024', 'Label': 'Accounts Payable - Intl'}
]

label_df = pd.DataFrame(label_data)


# === 8. Date Range Setup ===
today = datetime.now()
last_month = today - timedelta(days=3)
end_date = last_month.strftime('%Y-%m-%d')

start_month = (last_month - timedelta(days=450)).replace(day=1)
start_date = start_month.strftime('%Y-%m-%d')


# === 9. Data Collection: Cash Flow & P&L Only ===
main_data_dict = {}     # cf_ProjectName
main_data_dict_pl = {}  # pl_ProjectName

for company, zid in project_zid.items():
    print(f"Processing: {company} (zid={zid})")

    # --- Get GL Master ---
    df_master = get_gl_master(zid)

    # --- Cash Flow Report ---
    df_cf = get_cashflow_details(zid, company, start_date, end_date)
    if df_cf.empty:
        print(f"  ⚠️ No cash flow data for {company}")
    else:
        df_cf['time_line'] = df_cf['xyear'].astype(str) + '/' + df_cf['xper'].astype(str)
        df_cf_pvt = pd.pivot_table(
            df_cf,
            values='sum',
            index=['zid', 'xacc'],
            columns=['time_line'],
            aggfunc=np.sum
        ).reset_index().fillna(0)

        try:
            df_cf_pvt = df_master.merge(df_cf_pvt, on='xacc', how='right').fillna(0)
            df_cf_pvt = df_cf_pvt.sort_values('xhrc3')
            df_cf_pvt = df_cf_pvt.reset_index(drop=True)
            df_cf_pvt = df_cf_pvt.drop(['zid_x', 'zid_y'], axis=1, errors='ignore')
        except Exception as e:
            print(f"  ❌ Merge failed for {company}: {e}")
        else:
            # Safe function to add summary rows with on-the-fly mask
            def add_summary_row(df, col, val, label):
                mask = df[col].isin(val) if isinstance(val, list) else (df[col] == val)
                row = df.loc[mask].select_dtypes(include=[np.number]).sum(axis=0).copy()
                row['xhrc3'] = label
                for c in df.columns:
                    if c not in row.index and c != 'xhrc3':
                        row[c] = ''
                df.loc[len(df)] = row

            # Add summary rows
            add_summary_row(df_cf_pvt, 'xhrc3', ['Operating', 'Operating Investment'], 'Operating Cash Flow')
            add_summary_row(df_cf_pvt, 'xhrc3', 'Investing', 'Investing Cash Flow')
            add_summary_row(df_cf_pvt, 'xhrc3', 'Financing', 'Financing Cash Flow')

            # Free Cash Flow
            free_mask = (df_cf_pvt['xhrc3'] == 'Operating Cash Flow') | (df_cf_pvt['xhrc3'] == 'Investing Cash Flow')
            free_row = df_cf_pvt.loc[free_mask].select_dtypes(include=[np.number]).sum(axis=0)
            free_row['xhrc3'] = 'Free Cash Flow'
            for c in df_cf_pvt.columns:
                if c not in free_row and c != 'xhrc3':
                    free_row[c] = ''
            df_cf_pvt.loc[len(df_cf_pvt)] = free_row

            # Apply labels and reorder columns
            df_cf_pvt = df_cf_pvt.merge(label_df[['xacc', 'Label']], on='xacc', how='left')
            static_cols = [c for c in df_cf_pvt.columns if '/' not in c]
            time_cols = sorted([c for c in df_cf_pvt.columns if '/' in c], key=len)
            df_cf_pvt = df_cf_pvt[static_cols + time_cols]

            main_data_dict[zid] = df_cf_pvt

    # --- P&L Report ---
    df_pl = get_gl_details_project(zid, company, start_date, end_date)
    if df_pl.empty:
        print(f"  ⚠️ No P&L data for {company}")
    else:
        df_pl['time_line'] = df_pl['xyear'].astype(str) + '/' + df_pl['xper'].astype(str)
        df_pl_pvt = pd.pivot_table(
            df_pl,
            values='sum',
            index=['zid', 'xacc'],
            columns=['time_line'],
            aggfunc=np.sum
        ).reset_index().fillna(0)

        total_row = df_pl_pvt.select_dtypes(include=[np.number]).sum(axis=0)
        total_row['xacc'] = 'Profit/Loss'
        df_pl_pvt.loc[len(df_pl_pvt)] = total_row

        df_pl_pvt = df_master.merge(df_pl_pvt, on='xacc', how='right').fillna(0)
        df_pl_pvt = df_pl_pvt[(df_pl_pvt['xacctype'] != 'Asset') & (df_pl_pvt['xacctype'] != 'Liability')]
        df_pl_pvt = df_pl_pvt.drop(['zid_x', 'zid_y'], axis=1, errors='ignore')

        main_data_dict_pl[zid] = df_pl_pvt


# === 10. Export to Excel with Dynamic Sheet Names ===
def clean_sheet_name(name):
    """Clean name for Excel: remove invalid chars, limit to 31 chars."""
    name = re.sub(r'[/?:\[\]\\*]', '_', name)
    name = name.strip()
    return name[:31]

# Export Cash Flow
with pd.ExcelWriter('hmbr_cf.xlsx') as writer:
    for zid, df in main_data_dict.items():
        if zid in zid_to_name:
            company_name = zid_to_name[zid]
            safe_name = clean_sheet_name(company_name)
            sheet_name = f"cf_{safe_name}"
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        else:
            print(f"⚠️ No name found for zid={zid}")

# Export P&L
with pd.ExcelWriter('hmbr_pl.xlsx') as writer:
    for zid, df in main_data_dict_pl.items():
        if zid in zid_to_name:
            company_name = zid_to_name[zid]
            safe_name = clean_sheet_name(company_name)
            sheet_name = f"pl_{safe_name}"
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        else:
            print(f"⚠️ No name found for zid={zid}")


# === 11. Send Email ===
try:
    recipients = get_email_recipients(os.path.splitext(os.path.basename(__file__))[0])
    print(f"📬 Recipients: {recipients}")
except Exception as e:
    print(f"⚠️ Failed to get recipients: {e}. Using fallback.")
    recipients = ["ithmbrbd@gmail.com"]

today_str = datetime.now().strftime("%Y-%m-%d")
send_mail(
    subject=f"HM_02 HMBR Cashflow Statement -> {today_str}",
    bodyText="Please find the Cashflow and P&L reports attached.",
    attachment=['hmbr_cf.xlsx', 'hmbr_pl.xlsx'],
    recipient=recipients,
    html_body=None
)

engine.dispose()