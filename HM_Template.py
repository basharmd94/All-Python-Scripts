"""
🌟 SUBJECT : 

📅 Purpose:


🧠 Logic Flow:


🗃️ Tables Used:


📬 Recipients:


***** Note ****

"""


import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from dotenv import load_dotenv
import warnings

# === Load Environment & Config ===
load_dotenv()
ZID_HMBR = int(os.environ["ZID_GULSHAN_TRADING"])
ZID_GI = int(os.environ["ZID_GI"])
ZID_ZEPTO = int(os.environ["ZID_ZEPTO_CHEMICALS"])  # Zepto business

PROJECT_ROOT = os.path.dirname(os.getcwd())
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from mail import send_mail, get_email_recipients
from project_config import DATABASE_URL

engine = create_engine(DATABASE_URL)
warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)
pd.set_option('display.float_format', '{:.2f}'.format)


# === Helper: Fetch Order Data ===
def get_data(zid,):
    """Fetch confirmed/invoiced orders for given date and state(s)."""
    query = """

    """
    return pd.read_sql(query, engine, params=[zid, ])

 

# === Email ===

try:
    # Extract report name from filename
    report_name = os.path.splitext(os.path.basename(__file__))[0]
    recipients = get_email_recipients(report_name)
    print(f"📬 Recipients: {recipients}")
except Exception as e:
    print(f"⚠️ Failed to fetch recipients: {e}")
    recipients = ["ithmbrbd@gmail.com"]  # Fallback



body_text = """
    <h4> Dear Sir, </h4>
    <p>Please find the attached Excel/embeed HTML containing the subjective information.</p>

    <p><b>Best Regards,</b></p>
    <p>Automated Reporting System</p>
"""

send_mail(
    subject="HM_31: Customer Order List With Phone Number (District + Retail)",
    bodyText=body_text,
    attachment=['file.xlsx'],
    recipient=recipients,
    html_body = html_sections if len(html_sections) > 0 else [(df_new, f"Your Subject")]
)
print("📧 Email sent successfully")


# === Cleanup ===
engine.dispose()
print("✅ Process completed")