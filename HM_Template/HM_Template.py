"""
📦 

🚀 PURPOSE:


🏢 AFFECTED BUSINESSES:


📅 PERIOD:
    - January to current month of current year
📁 OUTPUT:


📬 EMAIL:


"""

import os
import sys
import pandas as pd
from datetime import datetime
import calendar
from dotenv import load_dotenv
from sqlalchemy import text  # ← Required for parameterized queries with SQLAlchemy

# ─────────────────────────────────────────────────────────────────────
# 🌍 1. Load Environment & Setup
# ─────────────────────────────────────────────────────────────────────
load_dotenv()

# Map ZIDs to Project Names using your .env


# ─────────────────────────────────────────────────────────────────────
# 🧩 2. Add Root & Import Shared Modules
# ─────────────────────────────────────────────────────────────────────
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from project_config import engine
from mail import send_mail, get_email_recipients


# ─────────────────────────────────────────────────────────────────────
# 📊 3. Helper: Get Last Day of Month
# ─────────────────────────────────────────────────────────────────────
def last_day_of_month(year: int, month: int) -> int:
    """Returns the last day of the given month."""
    return calendar.monthrange(year, month)[1]


# ─────────────────────────────────────────────────────────────────────
# 📥 4. Query Function: Get Inventory Value by Warehouse
# ─────────────────────────────────────────────────────────────────────
def get_inventory_value_by_warehouse(zid: int, as_of_date: str):
    """
    Fetch total inventory value per warehouse (xwh) up to a given date.
    Uses xval * xsign to account for correct transaction sign.
    Compatible with SQLAlchemy 1.4 + Pandas 2.3.2.
    """
    query = """
        SELECT 
            xwh,
            COALESCE(SUM(xval * xsign), 0) AS value
        FROM imtrn
        WHERE zid = :zid 
          AND xdate <= :as_of_date
        GROUP BY xwh
        ORDER BY xwh
    """
    return pd.read_sql(text(query), engine, params={
        'zid': zid,
        'as_of_date': as_of_date
    })


# ─────────────────────────────────────────────────────────────────────
# 📊 5. Generate Monthly Inventory Report
# ─────────────────────────────────────────────────────────────────────
print("📊 Generating monthly inventory value report...")

with pd.ExcelWriter(OUTPUT_FILE, engine="xlsxwriter") as writer:
    for zid, business_name in ZID_MAP.items():
        print(f"📊 Processing: {business_name} (ZID={zid})")
        monthly_data = []

        for month in range(1, CURRENT_MONTH + 1):
            last_day = last_day_of_month(YEAR, month)
            query_date = f"{YEAR}-{month:02d}-{last_day}"
            month_label = f"{calendar.month_name[month]}-{str(YEAR)[-2:]}"

            # Fetch data using reusable function
            try:
                df_month = get_inventory_value_by_warehouse(zid=zid, as_of_date=query_date)
                df_month.rename(columns={"value": month_label}, inplace=True)
            except Exception as e:
                print(f"❌ Error fetching data for {business_name}, month {month}: {e}")
                df_month = pd.DataFrame(columns=["xwh", month_label])

            if month == 1:
                monthly_data = df_month
            else:
                monthly_data = pd.merge(monthly_data, df_month, on="xwh", how="outer")

        # Clean up: fill NaN with 0
        monthly_data.fillna(0, inplace=True)

        # Excel-safe sheet name (max 31 chars)
        safe_sheet_name = business_name[:31]

        # Write to Excel sheet
        monthly_data.to_excel(writer, sheet_name=safe_sheet_name, index=False)

print(f"✅ Inventory report saved: {OUTPUT_FILE}")


# ─────────────────────────────────────────────────────────────────────
# 📬 6. Send Email (with HTML body)
# ─────────────────────────────────────────────────────────────────────
print("📧 Preparing email...")

try:
    # Extract report name from filename
    report_name = os.path.splitext(os.path.basename(__file__))[0]
    print(report_name)
    recipients = get_email_recipients(report_name)
    print(f"📬 Recipients: {recipients}")
except Exception as e:
    print(f"⚠️ Failed to fetch recipients: {e}")
    recipients = ["ithmbrbd@gmail.com"]  # Fallback


subject = f"HM_28 – Inventory Value by Warehouse ({YEAR})"

# HTML Email Body
business_list = "".join(f"<li><strong>{name}</strong> (ZID={zid})</li>" for zid, name in ZID_MAP.items())

body_text = f"""
<p>Dear Sir,</p>
<p>Please find the <strong>Monthly Inventory Value Report by Warehouse (xwh)</strong> attached.</p>
<p><strong>Period:</strong> January to {calendar.month_name[CURRENT_MONTH]} {YEAR}</p>
<p><strong>Businesses Included:</strong></p>
<ul>
{business_list}
</ul>
<p>The report shows monthly closing inventory values grouped by warehouse (xwh).</p>
<p>Best regards,<br>
Automated Reporting System</p>
"""

# Optional: Add summary tables in HTML
html_content = []
for name in ZID_MAP.values():
    summary_df = pd.DataFrame({"Business": [name], "Status": ["Report Included"]})
    html_content.append((summary_df, f"Summary: {name}"))

# Send email
try:
    send_mail(
        subject=subject,
        bodyText=body_text,
        attachment=[OUTPUT_FILE],
        recipient=recipients,
        html_body=html_content
    )
    print("✅ Email sent successfully.")
except Exception as e:
    print(f"❌ Failed to send email: {e}")
    raise