"""
🌟 SUBJECT : 

📅 Purpose:


🧠 Logic Flow:


🗃️ Tables Used:


📬 Recipients:
• Email: ithmbrbd@gmail.com,asaddat87@gmail.com,hmbronline@gmail.com
• Subject: HM_31: Customer Order List With Phone Number (District + Retail)

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

# === ZID Variables ===
ZID_ZEPTO = int(os.environ["ZID_ZEPTO_CHEMICALS"])
ZID_GI = int(os.environ["ZID_GI"])
ZID_HMBR = int(os.environ["ZID_GULSHAN_TRADING"])
ZID_ONLINE = int(os.environ["ZID_HMBR_ONLINE_SHOP"])
ZID_GROCERY = int(os.environ["ZID_HMBR_GROCERY"])
ZID_PACKAGING = int(os.environ["ZID_GULSHAN_PACKAGING"])


# === Helper: Fetch Order Data ===
def get_inventory_value_data(zid,):
    """Fetch Inventory value which balance is  less than 0"""
    query = """
            SELECT
            xitem as itemcode,
            xwh as warehouse,
                sum (xqty * xsign) AS qty,
                sum(xval * xsign) AS totalvalue
            FROM
                imtrn
            WHERE
                zid = %s
            GROUP BY xitem, xwh
            having   sum(xval * xsign) <= -200
            order by totalvalue asc

    """
    return pd.read_sql(query, engine, params=[zid, ])

# === ZID to Name ===
ZID_TO_NAME = {
    ZID_ZEPTO: "Zepto Chemicals",
    ZID_GI: "GI Corporation",
    ZID_HMBR: "GULSHAN TRADING",
    ZID_ONLINE: "Sales Warehouse Online Shop",
    ZID_GROCERY: "HMBR Grocery Shop",
    ZID_PACKAGING: "Gulshan Packaging"
}

# === loop through all zids and export data into a single excel file with seperate sheets for each zid ===
with pd.ExcelWriter('inventory_value_check.xlsx') as writer:
    for zid in [ZID_ZEPTO, ZID_GI, ZID_HMBR, ZID_ONLINE, ZID_GROCERY, ZID_PACKAGING]:
        df = get_inventory_value_data(zid)
        df.to_excel(writer, sheet_name=ZID_TO_NAME[zid], index=False)  # pyright: ignore[reportUndefinedVariable]
print("✅ Excel report generated: inventory_value_check.xlsx")

# ==== for html body create sepeate section of each zid ===
html_sections = []
for zid in [ZID_ZEPTO, ZID_GI, ZID_HMBR, ZID_ONLINE, ZID_GROCERY, ZID_PACKAGING]:
    df = get_inventory_value_data(zid)
    html_sections.append((df, ZID_TO_NAME[zid]))

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
<h3>Dear Sir,</h3>
<p>The following inventory items which balance are less than -200:</p>
<p style="font-weight: bold; color: red;"> Need to update the MO/TO/Warehouse as per the item actual Value</p>
<p>Please find the attached excel file for more details.</p>
<br>
<p>Best regards,<br>
<b>Automated Reporting System</b></p>


"""




send_mail(
    subject="HM_33: Inventory Item's Value Need Correction",
    bodyText=body_text,
    attachment=['inventory_value_check.xlsx'],
    recipient=recipients,
    html_body=html_sections if len(html_sections) > 0 else [(body_text, "Inventory Value Check")]
)
print("📧 Email sent successfully")

# === Cleanup ===
engine.dispose()
print("✅ Process completed")