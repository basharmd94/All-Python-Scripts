"""
📦 HM_27_AR_Report_By_Sales_Person.py – AR Summary by Salesperson (xsp)

🚀 PURPOSE:
    - Generate monthly Accounts Receivable (AR) report grouped by salesperson (xsp)
    - One sheet per business: GI, HMBR, Zepto
    - Export to Excel and email as attachment

🏢 AFFECTED BUSINESSES:
    - GI Corporation (ZID=100000)
    - HMBR (ZID=100001)
    - Zepto Chemicals (ZID=100005)

📅 PERIOD:
    - User-input year and month (e.g., 2025, 8 → August 2025)

📁 OUTPUT:
    - HM_27_AR_Report_YYYYMMDD.xlsx → One sheet per business
    - Email with Excel attachment and HTML summary

📬 EMAIL:
    - Recipients: get_email_recipients("HM_27_AR_Report_By_Sales_Person")
    - Subject: "HM_27 – AR Report by Salesperson"
    - Body: HTML with "Dear Sir", period, and report details
    - Attachment: HM_27_AR_Report_YYYYMMDD.xlsx

🔧 ENHANCEMENTS:
    - Interactive input for year and month
    - Clear month selection (1-Jan, 2-Feb, ...)
    - Uses .env, project_config.DATABASE_URL
    - Auto-detects recipients from filename
    - HM_27 prefix on file and subject
    - HTML email body with proper formatting
    - One-line cell documentation at the end
"""

import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


# ─────────────────────────────────────────────────────────────────────
# 🌍 1. Load Environment & Setup
# ─────────────────────────────────────────────────────────────────────
load_dotenv()

# Business mapping
ZID_MAP = {
    100000: "GI Corporation",
    100001: "HMBR",
    100005: "Zepto"
}

# Month names for display
MONTH_NAMES = [
    "1-Jan", "2-Feb", "3-Mar", "4-Apr", "5-May", "6-Jun",
    "7-Jul", "8-Aug", "9-Sep", "10-Oct", "11-Nov", "12-Dec"
]


# ─────────────────────────────────────────────────────────────────────
# 🧩 2. Add Root & Import Shared Modules
# ─────────────────────────────────────────────────────────────────────
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from project_config import DATABASE_URL
from mail import send_mail, get_email_recipients


# ─────────────────────────────────────────────────────────────────────
# 📥 3. User Input for Year and Month
# ─────────────────────────────────────────────────────────────────────
print("📅 AR Report by Salesperson (xsp)")
print("-------------------------------")

# Get year
while True:
    year_input = input("Enter year (e.g., 2025): ").strip()
    if year_input.isdigit() and len(year_input) == 4:
        year = int(year_input)
        break
    else:
        print("❌ Please enter a valid 4-digit year.")

# Get month
print("\nChoose month:")
for m in MONTH_NAMES:
    print(f"  {m}")
while True:
    month_input = input("\nEnter month number (1-12): ").strip()
    if month_input.isdigit():
        month = int(month_input)
        if 1 <= month <= 12:
            break
    print("❌ Please enter a number between 1 and 12.")

# Calculate start and end date of the selected month
start_date = datetime(year, month, 1)
end_date = (start_date + relativedelta(months=1) - timedelta(seconds=1)).strftime("%Y-%m-%d")
start_date = start_date.strftime("%Y-%m-%d")

print(f"\n📊 Fetching data for period: {start_date} to {end_date}")


# ─────────────────────────────────────────────────────────────────────
# ⚙️ 4. Create Database Engine
# ─────────────────────────────────────────────────────────────────────
engine = create_engine(DATABASE_URL)


# ─────────────────────────────────────────────────────────────────────
# 📊 5. SQL Query Template
# ─────────────────────────────────────────────────────────────────────
query_template = """
SELECT 
    gldetail.xsp, 
    prmst.xname, 
    SUM(gldetail.xamount) AS total_amount
FROM glheader
INNER JOIN gldetail ON glheader.xvoucher = gldetail.xvoucher
INNER JOIN prmst ON gldetail.xsp = prmst.xemp
WHERE 
    glheader.zid = :zid
    AND gldetail.zid = :zid
    AND prmst.zid = :zid
    AND gldetail.xaccusage = 'AR'
    AND glheader.xdate BETWEEN :start_date AND :end_date
GROUP BY gldetail.xsp, prmst.xname;
"""


# ─────────────────────────────────────────────────────────────────────
# 📥 6. Fetch Data for Each ZID
# ─────────────────────────────────────────────────────────────────────
print("📊 Fetching AR data by salesperson...")

dfs = {}

with engine.connect() as conn:
    for zid, name in ZID_MAP.items():
        print(f"Fetching data for {name} (zid={zid})...")
        result = conn.execute(text(query_template), {
            "zid": zid,
            "start_date": start_date,
            "end_date": end_date
        })
        df = pd.DataFrame(result.fetchall())
        if not df.empty:
            df.columns = result.keys()
        else:
            df = pd.DataFrame(columns=["xsp", "xname", "total_amount"])
        dfs[name] = df


# ─────────────────────────────────────────────────────────────────────
# 📁 7. Export to Excel
# ─────────────────────────────────────────────────────────────────────
output_excel = f"HM_27_AR_Report.xlsx"
print(f"📁 Writing Excel: {output_excel}...")

with pd.ExcelWriter(output_excel, engine='xlsxwriter') as writer:
    for sheet_name, df in dfs.items():
        safe_sheet_name = sheet_name[:31]  # Excel sheet name limit
        df.to_excel(writer, sheet_name=safe_sheet_name, index=False)

print(f"✅ Excel report saved: {output_excel}")


# ─────────────────────────────────────────────────────────────────────
# 📬 8. Send Email (with HTML body)
# ─────────────────────────────────────────────────────────────────────
print("📧 Preparing email...")

try:
    report_name = os.path.splitext(os.path.basename(__file__))[0]
    recipients = get_email_recipients(report_name)
    print(f"📬 Recipients: {recipients}")
except Exception as e:
    print(f"⚠️ Fallback: {e}")
    recipients = ["ithmbrbd@gmail.com"]

subject = f"HM_27 – AR Report by Salesperson ({year}-{month:02d})"

body_text = (
    "<p>Dear Sir,</p>"
    "<p>Please find attached the <strong>AR Report by Salesperson (xsp)</strong> for the month of:</p>"
    f"<p><strong>{MONTH_NAMES[month-1].split('-')[1]} {year}</strong></p>"
    "<p>This report includes total AR amounts grouped by salesperson for:</p>"
    "<ul>"
    "<li>GI Corporation</li>"
    "<li>HMBR</li>"
    "<li>Zepto Chemicals</li>"
    "</ul>"
    "<p>Best regards,<br>Automated Reporting System</p>"
)

# Create HTML summary for email body
html_content = []
for name, df in dfs.items():
    total = df['total_amount'].sum() if not df.empty else 0
    summary_df = pd.DataFrame({
        "Business": [name],
        "Total AR Amount": [f"{total:,.2f}"]
    })
    html_content.append((summary_df, f"Summary: {name}"))

send_mail(
    subject=subject,
    bodyText=body_text,
    attachment=[output_excel],
    recipient=recipients,
    html_body=html_content
)

print("✅ Email sent successfully.")