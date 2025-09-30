# %%
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
from datetime import date
import calendar
import os
import sys
from dotenv import load_dotenv
# ==================== Project config ====================
load_dotenv()
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from project_config import DATABASE_URL
from mail import send_mail, get_email_recipients

engine = create_engine(DATABASE_URL)

# ==================== CONFIGURATION ====================
# Define business units: zid -> friendly name
BUSINESS_UNITS = {
    100001: "Hmbr",
    100000: "GI",
    100005: "Zepto",
    # Add more as needed: zid: "Name"
}

# ==================== USER INPUT ====================
def get_user_input():
    """Prompt user for year and month range with validation."""
    while True:
        try:
            year = int(input("Enter the year (e.g., 2024): "))
            if 2000 <= year <= 2100:
                break
            print("Please enter a valid year between 2000 and 2100.")
        except ValueError:
            print("Please enter a valid year as a number.")

    while True:
        try:
            from_month = int(input("Enter the starting month (1-12): "))
            if 1 <= from_month <= 12:
                break
            print("Please enter a valid month between 1 and 12.")
        except ValueError:
            print("Please enter a valid month as a number.")

    while True:
        try:
            to_month = int(input("Enter the ending month (1-12): "))
            if 1 <= to_month <= 12 and to_month >= from_month:
                break
            print("Ending month must be >= starting month and between 1-12.")
        except ValueError:
            print("Please enter a valid month as a number.")

    return year, from_month, to_month

# Get date range
year, from_month, to_month = get_user_input()
_, last_day = calendar.monthrange(year, to_month)
start_date = f"{year}-{from_month:02d}-01"
end_date = f"{year}-{to_month:02d}-{last_day:02d}"
print(f"Processing data from {start_date} to {end_date}")

# ==================== DATA FETCHING FUNCTIONS ====================
def get_sales(zid, start_date, end_date, engine):
    """Fetch sales data for a given zid and date range."""
    query = text("""
        SELECT 
            opdor.xsp, 
            EXTRACT(YEAR FROM opdor.xdate)::INTEGER AS year,
            EXTRACT(MONTH FROM opdor.xdate)::INTEGER AS month,
            SUM(opddt.xlineamt) AS total_sales  
        FROM opdor
        LEFT JOIN opddt ON opdor.xdornum = opddt.xdornum
        WHERE opdor.zid = :zid
          AND opddt.zid = :zid
          AND opdor.xdate BETWEEN :start_date AND :end_date
          AND opdor.xstatusdor = '3-Invoiced'
        GROUP BY opdor.xsp, EXTRACT(YEAR FROM opdor.xdate), EXTRACT(MONTH FROM opdor.xdate)
        ORDER BY opdor.xsp, year, month
    """)
    return pd.read_sql(query, engine, params={"zid": zid, "start_date": start_date, "end_date": end_date})

def get_return(zid, start_date, end_date, engine):
    """Fetch standard return data."""
    query = text("""
        SELECT 
            opcrn.xemp AS xsp, 
            EXTRACT(YEAR FROM opcrn.xdate)::INTEGER AS year,
            EXTRACT(MONTH FROM opcrn.xdate)::INTEGER AS month,
            SUM(opcdt.xlineamt) AS total_return  
        FROM opcrn
        INNER JOIN opcdt ON opcrn.xcrnnum = opcdt.xcrnnum
        WHERE opcrn.zid = :zid
          AND opcdt.zid = :zid
          AND opcrn.xdate BETWEEN :start_date AND :end_date
        GROUP BY opcrn.xemp, EXTRACT(YEAR FROM opcrn.xdate), EXTRACT(MONTH FROM opcrn.xdate)
        ORDER BY xsp, year, month
    """)
    return pd.read_sql(query, engine, params={"zid": zid, "start_date": start_date, "end_date": end_date})

def get_return_reca(zid, start_date, end_date, engine):
    """Fetch RECA-type returns."""
    query = text("""
        SELECT 
            imtemptrn.xemp AS xsp, 
            EXTRACT(YEAR FROM imtemptrn.xdate)::INTEGER AS year,
            EXTRACT(MONTH FROM imtemptrn.xdate)::INTEGER AS month,
            SUM(imtemptdt.xval) AS reca_amt  
        FROM imtemptrn 
        JOIN imtemptdt ON imtemptrn.ximtmptrn = imtemptdt.ximtmptrn
        WHERE imtemptrn.zid = :zid 
          AND imtemptdt.zid = :zid 
          AND imtemptrn.ximtmptrn LIKE '%RECA%'
          AND imtemptrn.xdate BETWEEN :start_date AND :end_date
        GROUP BY imtemptrn.xemp, EXTRACT(YEAR FROM imtemptrn.xdate), EXTRACT(MONTH FROM imtemptrn.xdate)
        ORDER BY xsp, year, month
    """)
    return pd.read_sql(query, engine, params={"zid": zid, "start_date": start_date, "end_date": end_date})

def get_salesman(engine):
    """Fetch salesman master data for all zids in BUSINESS_UNITS."""
    zids = list(BUSINESS_UNITS.keys())
    query = text("SELECT xemp, xname FROM prmst WHERE zid = ANY(:zids)")
    return pd.read_sql(query, engine, params={"zids": zids})

# ==================== MAIN PROCESSING ====================
# Fetch and deduplicate salesman data
df_salesman = get_salesman(engine)
df_salesman = df_salesman.drop_duplicates(subset=['xemp']).reset_index(drop=True)

# Dictionary to store processed data per business
business_data = {}

# Process each business unit
for zid, biz_name in BUSINESS_UNITS.items():
    print(f"Fetching data for {biz_name} (zid={zid})...")
    
    # Fetch raw data
    df_sales = get_sales(zid, start_date, end_date, engine)
    df_return = get_return(zid, start_date, end_date, engine)
    df_reca = get_return_reca(zid, start_date, end_date, engine)
    
    # Ensure each dataset is aggregated to one row per (xsp, year, month)
    df_sales = df_sales.groupby(['xsp', 'year', 'month'], as_index=False).sum()
    df_return = df_return.groupby(['xsp', 'year', 'month'], as_index=False).sum()
    df_reca = df_reca.groupby(['xsp', 'year', 'month'], as_index=False).sum()
    
    # Merge returns: standard + RECA
    df_return_full = pd.merge(
        df_return, df_reca,
        on=['xsp', 'year', 'month'],
        how='outer'
    ).fillna(0)
    df_return_full['total_return'] = df_return_full['total_return'] + df_return_full['reca_amt']
    df_return_full = df_return_full[['xsp', 'year', 'month', 'total_return']]
    
    # Merge sales and returns
    df_net = pd.merge(
        df_sales, df_return_full,
        on=['xsp', 'year', 'month'],
        how='outer'
    ).fillna(0)
    
    # Final deduplication (safety net)
    df_net = df_net.groupby(['xsp', 'year', 'month'], as_index=False).sum()
    
    # Rename columns with business name
    df_net = df_net.rename(columns={
        'total_sales': f'total_sales_{biz_name}',
        'total_return': f'total_return_{biz_name}'
    })
    
    business_data[biz_name] = df_net

# ==================== MERGE ALL BUSINESS UNITS ====================
biz_names = list(BUSINESS_UNITS.values())
df_combined = business_data[biz_names[0]].copy()

for biz in biz_names[1:]:
    df_combined = pd.merge(
        df_combined,
        business_data[biz],
        on=['xsp', 'year', 'month'],
        how='outer'
    ).fillna(0)

# Final deduplication after full merge
df_combined = df_combined.groupby(['xsp', 'year', 'month'], as_index=False).sum()

# ==================== ADD SALESMAN NAMES ====================
df_combined = pd.merge(
    df_combined,
    df_salesman,
    how='left',
    left_on='xsp',
    right_on='xemp'
).drop(columns=['xemp'])

# Fill missing salesman names (if any)
df_combined['xname'] = df_combined['xname'].fillna(df_combined['xsp'])

# ==================== ADD MONTH NAMES ====================
month_map = {
    1: 'January', 2: 'February', 3: 'March', 4: 'April',
    5: 'May', 6: 'June', 7: 'July', 8: 'August',
    9: 'September', 10: 'October', 11: 'November', 12: 'December'
}
df_combined['month_name'] = df_combined['month'].map(month_map)

# ==================== CALCULATE NET SALES ====================
sales_cols = [col for col in df_combined.columns if col.startswith('total_sales_')]
return_cols = [col for col in df_combined.columns if col.startswith('total_return_')]
df_combined['net_sales'] = df_combined[sales_cols].sum(axis=1) - df_combined[return_cols].sum(axis=1)
# ==================== REORDER COLUMNS ====================
base_cols = ['xsp', 'xname', 'year', 'month_name']
dynamic_cols = sorted([col for col in df_combined.columns if col not in base_cols + ['month', 'net_sales']])
final_cols = base_cols + dynamic_cols + ['net_sales']
df_combined = df_combined[final_cols]

# ==================== ADD SUMMARIES ====================
# Monthly totals
monthly_summary = df_combined.groupby(['year', 'month_name'], as_index=False).agg({
    **{col: 'sum' for col in dynamic_cols + ['net_sales']},
    'xsp': lambda _: 'Monthly Total',
    'xname': lambda g: f"{g.iloc[0]}-{g.name[1]}"
})

# Grand total
grand_total = {
    'xsp': 'Grand Total',
    'xname': f"Period: {start_date} to {end_date}",
    'year': '',
    'month_name': ''
}
for col in dynamic_cols + ['net_sales']:
    grand_total[col] = df_combined[col].sum()

# Combine all
df_final = pd.concat([
    df_combined,
    monthly_summary,
    pd.DataFrame([grand_total])
], ignore_index=True)

# ==================== EXPORT ====================
output_filename = f"Salesman_Net_Sales_{year}_{from_month:02d}_to_{to_month:02d}.xlsx"
df_final.to_excel(output_filename, index=False)

print(f"\n✅ Report saved successfully: {output_filename}")
print(f"📊 Final rows: {len(df_final)} | Unique (xsp, year, month_name): {df_combined[['xsp', 'year', 'month_name']].drop_duplicates().shape[0]}")

try:
    # Extract report name from filename
    report_name = os.path.splitext(os.path.basename(__file__))[0]
    recipients = get_email_recipients(report_name)
    print(f"📬 Recipients: {recipients}")
except Exception as e:
    print(f"⚠️ Failed to fetch recipients: {e}")
    recipients = ["ithmbrbd@gmail.com"]  # Fallback



# Optional: send email
send_mail(
    subject=f"HM_35 Salesman Wise Net Sales Report ({start_date} to {end_date})",
    bodyText=f"Please find the attached report for period {start_date} to {end_date}.",
    attachment=[output_filename],
    recipient=recipients
)