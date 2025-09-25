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
# 📬 6. Send Email (with HTML body)
# ─────────────────────────────────────────────────────────────────────
print("📧 Preparing email...")

try:
    # Extract report name from filename
    report_name = os.path.splitext(os.path.basename(__file__))[0]
    print(report_name)
    # recipients = get_email_recipients(report_name)
    recipients = ["ithmbrbd@gmail.com"]  # Fallback
    print(f"📬 Recipients: {recipients}")
except Exception as e:
    print(f"⚠️ Failed to fetch recipients: {e}")
    recipients = ["ithmbrbd@gmail.com"]  # Fallback


subject = f"HM_28 – Inventory Value by Warehouse"

# Send email
try:
    send_mail(
        subject=subject,
        bodyText="Hello this is test",
        recipient=recipients,
    )
    print("✅ Email sent successfully.")
except Exception as e:
    print(f"❌ Failed to send email: {e}")
    raise