"""
🚀 HM_08_03_Store_Dispatch_Followup.py – Daily List of DOs Needing Dispatch Date

📌 PURPOSE:
    - Fetch DOs from previous working day (skip Friday → Thursday)
    - Status: '1-Open' or '2-Confirmed' (not dispatched)
    - Send to store team for dispatch date entry
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
    ZID = int(os.environ["ZID_GULSHAN_TRADING"])
except KeyError:
    raise RuntimeError("❌ ZID_GULSHAN_TRADING not found in .env")

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


# === 6. Determine Previous Working Day ===
now = datetime.now()
prev = now - timedelta(days=2 if now.weekday() == 0 else 1)  # Mon → Fri, others → yesterday
PREV_DATE = prev.strftime("%Y-%m-%d")
PREV_DAY = prev.strftime("%A")

print(f"📅 Processing DOs from: {PREV_DATE} ({PREV_DAY})")


# === 7. Salesman List (for filtering) ===
salesman_ids = (
    'SA--000068',
    'SA--000224',
    'SA--000038',
    'SA--000144',
    'SA--000021',
    'SA--000193',
    'SA--000114',
    'SA--000011',
    'SA--000192',
    'SA--000098',
    'SA--000227',
    'SA--000242'
)


# === 8. Fetch DOs Needing Dispatch Date ===
query = text("""
    SELECT 
        opdor.xdornum AS do_number,
        opdor.xdate AS do_date,
        opdor.xcus AS customer_id,
        cacus.xshort AS customer_name,
        cacus.xcity AS city,
        cacus.xstate AS area,
        opdor.xsp AS salesman_id,
        prmst.xname AS salesman_name,
        opdor.xtotamt AS total_amount,
        opdor.xstatusdor AS status
    FROM opdor
    LEFT JOIN cacus ON opdor.xcus = cacus.xcus
    LEFT JOIN prmst ON opdor.xsp = prmst.xemp
    WHERE opdor.zid = :zid
      AND opdor.xdate = :prev_date
      AND opdor.xstatusdor IN ('1-Open', '2-Confirmed')
      AND opdor.xsp IN :salesman_ids
    ORDER BY opdor.xdornum
""")

try:
    df = pd.read_sql(query, engine, params={'zid': ZID, 'prev_date': PREV_DATE, 'salesman_ids': salesman_ids})
except Exception as e:
    raise RuntimeError(f"❌ Database query failed: {e}")

if df.empty:
    print("📭 No DOs found needing dispatch date.")
    sys.exit(0)

print(f"✅ Found {len(df)} DOs from {PREV_DATE} needing dispatch date.")


# === 9. Export to Excel ===
filename = f"HM_08_03_{PREV_DATE}.xlsx"
df.to_excel(filename, index=False, engine='openpyxl')
print(f"📁 Saved: {filename}")


# === 10. Send Email ===
try:
    recipients = get_email_recipients(os.path.splitext(os.path.basename(__file__))[0])
    print(f"📬 Recipients: {recipients}")
except Exception as e:
    print(f"⚠️ Failed to get recipients: {e}. Using fallback.")
    recipients = ["ithmbrbd@gmail.com"]
    
subject = f"HM_08.3 Store: Fill Dispatch Date for DOs from {PREV_DATE}"
body_text = f"""
Dear Store Team,

Please fill the dispatch date for the attached DOs from {PREV_DATE}.
These orders are still in 'Open' or 'Confirmed' status.

Thank you.
"""

send_mail(
    subject=subject,
    bodyText=body_text,
    attachment=[filename],
    recipient=recipients,
    html_body=None
)


# === 11. Cleanup ===
engine.dispose()
print("✅ HM_08.3 completed successfully.")