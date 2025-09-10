"""
HM_03_Customer_n_Item.py – Customer & Item Master Export
Only for GULSHAN TRADING (ZID_GULSHAN_TRADING)
"""

import os
import sys
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime

# === 1. Load .env file ===
load_dotenv()

# === 2. Read ZID directly (fail if missing or invalid) ===
ZID = int(os.environ["ZID_GULSHAN_TRADING"])
print(f"📌 Running for ZID_GULSHAN_TRADING = {ZID}")


# === 3. Add root to path (E:\) ===
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


# === 4. Import shared modules ===
from mail import send_mail, get_email_recipients
from project_config import DATABASE_URL


# === 5. Create DB engine ===
engine = create_engine(DATABASE_URL)


# === 6. Suppress warnings ===
import warnings
warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)


# === 7. Fetch customer data ===
def get_customer(zid):
    query = """
        SELECT xcus AS customerID, 
               xshort AS customerName, 
               xadd2 AS address,
               xcity AS city, 
               xstate AS area 
        FROM cacus
        WHERE zid = %s
    """
    return pd.read_sql(query, engine, params=[zid])

# === 8. Fetch item data ===
def get_item(zid):
    query = """
        SELECT xitem AS itemCode, 
               xdesc AS itemName, 
               xunitstk AS unit, 
               xgitem AS itemGroup1, 
               xabc AS itemGroupXabc 
        FROM caitem
        WHERE zid = %s
    """
    return pd.read_sql(query, engine, params=[zid])


# === 9. Fetch data ===
df_customer = get_customer(ZID)
df_item = get_item(ZID)

print(f"✅ Fetched {len(df_customer)} customers and {len(df_item)} items")


# === 10. Export to Excel ===
filename = f"CustomerAndItemList_{ZID}.xlsx"
with pd.ExcelWriter(filename) as writer:
    df_customer.to_excel(writer, sheet_name="Customer", index=False)
    df_item.to_excel(writer, sheet_name="Item", index=False)

print(f"📁 Saved: {filename}")


# === 11. Send email ===
try:
    recipients = get_email_recipients('HM_03_Customer_n_Item')
    print(f"📬 Recipients: {recipients}")
except Exception as e:
    print(f"⚠️ Failed to get recipients: {e}. Using fallback.")
    recipients = ["ithmbrbd@gmail.com"]


today_str = datetime.now().strftime("%Y-%m-%d")
send_mail(
    subject=f"HM_03 Customer & Item List – ZID {ZID} Date: {today_str} ",
    bodyText=f"Customer and item master data for ZID {ZID} (GULSHAN TRADING) attached.",
    attachment=[filename],
    recipient=recipients,
    html_body=None
)


# === 12. Cleanup ===
engine.dispose()