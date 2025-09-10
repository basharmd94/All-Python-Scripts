"""
📊 HM_19_Sales_Target_vs_Achievement.py – Monthly Sales Target vs Achievement

🚀 PURPOSE:
    - Analyze HMBR, GI, and Zepto sales performance
    - Compare actual sales vs targets (market-wise & salesman-wise)
    - Include 24-month customer history
    - Export to Excel + send HTML email summary

🏢 AFFECTED BUSINESSES:
    - HMBR (Gulshan Trading, ZID=100001)
    - GI Corporation (ZID=100000)
    - Zepto Chemicals (ZID=100005)

📅 PERIOD:
    - Current month (auto-detected)
    - Last 24 months for customer history
    - Last 6 months for trend analysis

📁 OUTPUT:
    - HM_19_Sales_Target_vs_Achievement.xlsx (8 sheets)
    - Email with HTML summary table

📬 EMAIL:
    - Recipients: get_email_recipients("HM_19_Sales_Target_vs_Achievement")
    - Fallback: ithmbrbd@gmail.com
    - Subject: "HMBR Overall Target"
    - Body: HTML table with conditional formatting

🎯 TARGET DATA:
    - Market-wise: from aws_target.xlsx
    - Salesman-wise: from sw_target.xlsx
"""

import os
import sys
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv


# ─────────────────────────────────────────────────────────────────────
# 🌍 1. Load Environment & Setup
# ─────────────────────────────────────────────────────────────────────
load_dotenv()

print("🌍 Loading configuration...")

# Load ZIDs from .env
try:
    ZID_HMBR = int(os.environ["ZID_GULSHAN_TRADING"])    # 100001
    ZID_GI = int(os.environ["ZID_GI"])                   # 100000 → GI Corporation
    ZID_ZEPTO = int(os.environ["ZID_ZEPTO_CHEMICALS"])   # 100005
except KeyError as e:
    raise RuntimeError(f"❌ Missing ZID in .env: {e}")

# Projects
PROJECT_HMBR = "GULSHAN TRADING"
PROJECT_GI = "GI Corporation"
PROJECT_ZEPTO = "Zepto Chemicals"

# Date Setup
this_datetime = datetime.now()
number_day = this_datetime.day

# Last 24 months for history
month_list_24 = [(this_datetime - relativedelta(months=i)).strftime('%Y/%m') for i in range(24)]
start_year = int(month_list_24[-1].split('/')[0])
start_month = int(month_list_24[-1].split('/')[1])
end_year = int(month_list_24[0].split('/')[0])
end_month = int(month_list_24[0].split('/')[1])

print(f"📅 Reporting Period: {start_year}/{start_month:02d} → {end_year}/{end_month:02d}")


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
# 📥 4. Fetch Master Data
# ─────────────────────────────────────────────────────────────────────
def get_customers(zid):
    """Fetch customer details."""
    query = text("SELECT xcus, xshort, xadd2, xcity, xstate FROM cacus WHERE zid = :zid ORDER BY xcus")
    return pd.read_sql(query, engine, params={'zid': zid})

def get_sales(zid, start_year, start_month):
    """Fetch sales (DO--) for 24-month period."""
    year_month = f"{start_year}{start_month:02d}"
    query = text("""
        SELECT imtrn.xcus, imtrn.xitem, imtrn.xyear, imtrn.xper, imtrn.xdate, imtrn.xqty,
               opddt.xrate, opddt.xlineamt, opdor.xsp, opdor.xdiscamt, opdor.xtotamt
        FROM imtrn
        JOIN opddt ON imtrn.xdocnum = opddt.xdornum AND imtrn.xitem = opddt.xitem
        JOIN opdor ON imtrn.xdocnum = opdor.xdornum
        WHERE imtrn.zid = :zid
          AND opddt.zid = :zid
          AND opdor.zid = :zid
          AND CONCAT(imtrn.xyear, LPAD(imtrn.xper::text, 2, '0')) >= :year_month
          AND imtrn.xdoctype = 'DO--'
    """)
    return pd.read_sql(query, engine, params={'zid': zid, 'year_month': year_month})

def get_returns(zid, start_year, start_month):
    """Fetch returns (SR--) for 24-month period."""
    year_month = f"{start_year}{start_month:02d}"
    query = text("""
        SELECT imtrn.xcus, imtrn.xitem, imtrn.xyear, imtrn.xper, imtrn.xdate, imtrn.xqty,
               opcdt.xrate, (opcdt.xrate * imtrn.xqty) AS totamt, opcrn.xemp AS xsp
        FROM imtrn
        JOIN opcdt ON imtrn.xdocnum = opcdt.xcrnnum AND imtrn.xitem = opcdt.xitem
        JOIN opcrn ON imtrn.xdocnum = opcrn.xcrnnum
        WHERE imtrn.zid = :zid
          AND opcdt.zid = :zid
          AND opcrn.zid = :zid
          AND CONCAT(imtrn.xyear, LPAD(imtrn.xper::text, 2, '0')) >= :year_month
          AND imtrn.xdoctype = 'SR--'
    """)
    return pd.read_sql(query, engine, params={'zid': zid, 'year_month': year_month})

def get_employees(zid):
    """Fetch active sales employees."""
    query = text("SELECT xemp, xname, xdept, xdesig, xstatusemp FROM prmst WHERE zid = :zid")
    df = pd.read_sql(query, engine, params={'zid': zid})
    df = df.rename(columns={'xemp': 'xsp'})
    return df


# ─────────────────────────────────────────────────────────────────────
# 📊 5. Load Target Data
# ─────────────────────────────────────────────────────────────────────
print("📊 Loading target data...")

# Market-wise target
df_overall_target = pd.read_excel('aws_target.xlsx', engine='openpyxl')
df_overall_target = df_overall_target.iloc[:, 1:4]
df_overall_target = df_overall_target.groupby('Market')['HMBR'].sum().reset_index()
df_overall_target = df_overall_target.rename(columns={'Market': 'xstate', 'HMBR': 'Target'})

# Salesman-wise target
df_salesman_target = pd.read_excel('sw_target.xlsx', engine='openpyxl')
df_salesman_target = df_salesman_target.iloc[:, 0:3]
df_salesman_target = df_salesman_target.rename(columns={'Employee Code': 'xsp'})

print("✅ Targets loaded.")


# ─────────────────────────────────────────────────────────────────────
# 🧮 6. Process HMBR Sales
# ─────────────────────────────────────────────────────────────────────
print("📊 Processing HMBR data...")
df_cus_h = get_customers(ZID_HMBR)
df_sales_h = get_sales(ZID_HMBR, start_year, start_month)
df_return_h = get_returns(ZID_HMBR, start_year, start_month)
df_emp_h = get_employees(ZID_HMBR)
df_emp_h = df_emp_h[df_emp_h['xstatusemp'] == 'A-Active']
df_emp_h = df_emp_h[df_emp_h['xdept'].isin(['Sales & Marketing', 'Marketing', 'Sales'])]

# Sales & Returns
df_sales_g_h = df_sales_h.groupby(['xcus', 'xyear', 'xper', 'xsp'])['xlineamt'].sum().round(2).reset_index()
df_return_g_h = df_return_h.groupby(['xcus', 'xyear', 'xper', 'xsp'])['totamt'].sum().round(2).reset_index()

df_hmbr_g_h = df_cus_h.merge(df_sales_g_h, on='xcus', how='left') \
                      .merge(df_return_g_h, on=['xcus', 'xyear', 'xper', 'xsp'], how='left') \
                      .fillna(0)
df_hmbr_g_h['HMBR'] = df_hmbr_g_h['xlineamt'] - df_hmbr_g_h['totamt']
df_hmbr_g_h['time_line'] = df_hmbr_g_h['xyear'].astype(str) + '/' + df_hmbr_g_h['xper'].astype(str).str.zfill(2)

df_hmbr_customer = pd.pivot_table(df_hmbr_g_h, values='HMBR', index=['xcus', 'xshort', 'xcity'],
                                  columns='time_line', aggfunc='sum').reset_index().fillna(0)
df_hmbr_salesman = pd.pivot_table(df_hmbr_g_h, values='HMBR', index='xsp', columns='time_line',
                                  aggfunc='sum').reset_index().fillna(0)
df_hmbr_salesman = df_hmbr_salesman.merge(df_emp_h[['xsp', 'xname']], on='xsp', how='left')

# Current month summary
df_hmbr_overall = df_hmbr_g_h[(df_hmbr_g_h['xyear'] == end_year) & (df_hmbr_g_h['xper'] == end_month)]
df_hmbr_overall = df_hmbr_overall.groupby('xstate')['HMBR'].sum().round(2).reset_index()


# ─────────────────────────────────────────────────────────────────────
# 🧮 7. Process Zepto Sales (HMBR Channel)
# ─────────────────────────────────────────────────────────────────────
print("📊 Processing Zepto data...")
df_cus_z = get_customers(ZID_ZEPTO)
df_sales_z = get_sales(ZID_ZEPTO, start_year, start_month)
df_return_z = get_returns(ZID_ZEPTO, start_year, start_month)
df_emp_z = get_employees(ZID_ZEPTO)
df_emp_z = df_emp_z.rename(columns={'xsp': 'xsp'})
df_emp_z['businessId'] = 'HMBR'
df_emp_z.loc[df_emp_z['xsp'].str.startswith(('AD', 'EC', 'RD')), 'businessId'] = 'Other'

# Filter only HMBR-related sales
df_sales_z = df_sales_z.merge(df_emp_z[['xsp', 'businessId']], on='xsp', how='left')
df_sales_z = df_sales_z[df_sales_z['businessId'] == 'HMBR']
df_return_z = df_return_z.merge(df_emp_z[['xsp', 'businessId']], on='xsp', how='left')
df_return_z = df_return_z[df_return_z['businessId'] == 'HMBR']

# Aggregate
df_sales_g_z = df_sales_z.groupby(['xcus', 'xyear', 'xper', 'xsp'])['xlineamt'].sum().round(2).reset_index()
df_return_g_z = df_return_z.groupby(['xcus', 'xyear', 'xper', 'xsp'])['totamt'].sum().round(2).reset_index()

df_zepto_g_z = df_cus_h.merge(df_sales_g_z, on='xcus', how='left') \
                      .merge(df_return_g_z, on=['xcus', 'xyear', 'xper', 'xsp'], how='left') \
                      .fillna(0)
df_zepto_g_z['Zepto'] = df_zepto_g_z['xlineamt'] - df_zepto_g_z['totamt']
df_zepto_g_z['time_line'] = df_zepto_g_z['xyear'].astype(str) + '/' + df_zepto_g_z['xper'].astype(str).str.zfill(2)

df_zepto_customer = pd.pivot_table(df_zepto_g_z, values='Zepto', index=['xcus', 'xshort', 'xcity'],
                                   columns='time_line', aggfunc='sum').reset_index().fillna(0)
df_zepto_salesman = pd.pivot_table(df_zepto_g_z, values='Zepto', index='xsp', columns='time_line',
                                   aggfunc='sum').reset_index().fillna(0)
df_zepto_salesman = df_zepto_salesman.merge(df_emp_z[['xsp', 'xname']], on='xsp', how='left')

# Current month (exclude specific salesmen)
remove_salesman_list = [
    'SA--000446', 'SA--000100', 'SA--000431', 'SA--000443', 'SA--000440', 'SA--000425',
    'SA--000200', 'SA--000421', 'SA--000199', 'SA--000427', 'SA--000448', 'SA--000409',
    'SA--000194', 'SA--000196', 'SA--000428', 'SA--000430'
]
df_zepto_overall = df_zepto_g_z[(df_zepto_g_z['xyear'] == end_year) & (df_zepto_g_z['xper'] == end_month)]
df_zepto_overall = df_zepto_overall[~df_zepto_overall['xsp'].isin(remove_salesman_list)]
df_zepto_overall = df_zepto_overall.groupby('xstate')['Zepto'].sum().round(2).reset_index()


# ─────────────────────────────────────────────────────────────────────
# 🧮 8. Process GI Sales
# ─────────────────────────────────────────────────────────────────────
print("📊 Processing GI data...")
df_cus_gi = get_customers(ZID_GI)
df_sales_gi = get_sales(ZID_GI, start_year, start_month)
df_return_gi = get_returns(ZID_GI, start_year, start_month)
df_emp_gi = get_employees(ZID_GI)

df_sales_g_gi = df_sales_gi.groupby(['xcus', 'xyear', 'xper', 'xsp'])['xlineamt'].sum().round(2).reset_index()
df_return_g_gi = df_return_gi.groupby(['xcus', 'xyear', 'xper', 'xsp'])['totamt'].sum().round(2).reset_index()

df_gi_g = df_cus_h.merge(df_sales_g_gi, on='xcus', how='left') \
                  .merge(df_return_g_gi, on=['xcus', 'xyear', 'xper', 'xsp'], how='left') \
                  .fillna(0)
df_gi_g['GI'] = df_gi_g['xlineamt'] - df_gi_g['totamt']
df_gi_g['time_line'] = df_gi_g['xyear'].astype(str) + '/' + df_gi_g['xper'].astype(str).str.zfill(2)

df_gi_customer = pd.pivot_table(df_gi_g, values='GI', index=['xcus', 'xshort', 'xcity'],
                                columns='time_line', aggfunc='sum').reset_index().fillna(0)
df_gi_salesman = pd.pivot_table(df_gi_g, values='GI', index='xsp', columns='time_line',
                                aggfunc='sum').reset_index().fillna(0)
df_gi_salesman = df_gi_salesman.merge(df_emp_gi[['xsp', 'xname']], on='xsp', how='left')

df_gi_overall = df_gi_g[(df_gi_g['xyear'] == end_year) & (df_gi_g['xper'] == end_month)]
df_gi_overall = df_gi_overall.groupby('xstate')['GI'].sum().round(2).reset_index()


# ─────────────────────────────────────────────────────────────────────
# 📊 9. Combine & Calculate Achievement
# ─────────────────────────────────────────────────────────────────────
print("📊 Calculating target vs achievement...")

# Merge all
df_final = df_hmbr_overall.merge(df_zepto_overall, on='xstate', how='left') \
                          .merge(df_gi_overall, on='xstate', how='left') \
                          .merge(df_overall_target, on='xstate', how='left').fillna(0)

df_final['Total_net_Sales'] = df_final['HMBR'] + df_final['Zepto'] + df_final['GI']
df_final['Difference'] = df_final['Target'] - df_final['Total_net_Sales']
df_final['% Achievement'] = (df_final['Total_net_Sales'] / df_final['Target']) * 100

df_final = df_final[['xstate', 'HMBR', 'GI', 'Zepto', 'Total_net_Sales', 'Target', 'Difference', '% Achievement']]

# Add totals
total_row = {
    'xstate': 'Grand_Total',
    'HMBR': df_final['HMBR'].sum(),
    'GI': df_final['GI'].sum(),
    'Zepto': df_final['Zepto'].sum(),
    'Total_net_Sales': df_final['Total_net_Sales'].sum(),
    'Target': df_final['Target'].sum(),
    'Difference': df_final['Difference'].sum(),
    '% Achievement': (df_final['Total_net_Sales'].sum() / df_final['Target'].sum()) * 100
}
df_final = pd.concat([df_final, pd.DataFrame([total_row])], ignore_index=True)

# Required & Gap
required = {'xstate': '', 'HMBR': 'Overall', 'Total_net_Sales': 'Required %', '% Achievement': (100 / 30.5) * number_day}
gap = {'xstate': '', 'HMBR': 'Overall', 'Total_net_Sales': 'Gap %', '% Achievement': df_final.iloc[-1]['% Achievement'] - required['% Achievement']}
df_final = pd.concat([df_final, pd.DataFrame([required, gap])], ignore_index=True).round(2)

# Salesman achievement
last_col = df_hmbr_salesman.columns[-2]
df_hmbr_sales_t = df_hmbr_salesman[['xsp', last_col]].merge(df_emp_gi[['xsp', 'xname']], on='xsp', how='left') \
                                                      .merge(df_salesman_target[['xsp', 'Employee Name', 'Monthly Target']], on='xsp', how='left') \
                                                      .fillna(0)
df_hmbr_sales_t = df_hmbr_sales_t[df_hmbr_sales_t['Employee Name'] != 0]
df_hmbr_sales_t['% Achieved'] = (df_hmbr_sales_t[last_col] / df_hmbr_sales_t['Monthly Target']) * 100
df_hmbr_sales_t = df_hmbr_sales_t[['xsp', 'Employee Name', last_col, 'Monthly Target', '% Achieved']].round(2)


# ─────────────────────────────────────────────────────────────────────
# 📁 10. Export to Excel
# ─────────────────────────────────────────────────────────────────────
OUTPUT_FILE = "HM_19_Sales_Target_vs_Achievement.xlsx"

print(f"📁 Writing to {OUTPUT_FILE}...")
with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
    df_final.to_excel(writer, sheet_name='HmbrOverAllSummary', index=False)
    df_hmbr_sales_t.to_excel(writer, sheet_name='HmbrOverAllSaleSummary', index=False)
    df_hmbr_customer.to_excel(writer, sheet_name='HmbrCustomerWise', index=False)
    df_hmbr_salesman.to_excel(writer, sheet_name='HmbrSalesManWise', index=False)
    df_gi_customer.to_excel(writer, sheet_name='GICustomerWise', index=False)
    df_gi_salesman.to_excel(writer, sheet_name='GISalesManWise', index=False)
    df_zepto_customer.to_excel(writer, sheet_name='ZeptoCustomerWise', index=False)
    df_zepto_salesman.to_excel(writer, sheet_name='ZeptoSalesManWise', index=False)

print(f"✅ Excel saved: {OUTPUT_FILE}")


# ─────────────────────────────────────────────────────────────────────
# 📬 11. Send Email
# ─────────────────────────────────────────────────────────────────────
try:
    recipients = get_email_recipients(os.path.splitext(os.path.basename(__file__))[0])
    print(f"📬 Recipients: {recipients}")
except Exception as e:
    print(f"⚠️ Fallback: {e}")
    recipients = ["ithmbrbd@gmail.com"]

subject = "HMBR Overall Target"
body_text = """
Dear Sir,

Please find the monthly sales target vs achievement report.

Includes:
- Market-wise performance (HMBR, GI, Zepto)
- Salesman-wise achievement
- 24-month customer history

Full details in attachment.
"""

send_mail(
    subject=subject,
    bodyText=body_text,
    attachment=[OUTPUT_FILE],
    recipient=recipients,
    html_body=[(df_final, "HMBR Overall Summary")]
)

print("✅ HM_19 completed successfully.")