
import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from dotenv import load_dotenv
import warnings

# === Load Environment & Config ===
load_dotenv()


PROJECT_ROOT = os.path.dirname(os.getcwd())
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from mail import send_mail, get_email_recipients
from project_config import DATABASE_URL

engine = create_engine(DATABASE_URL)
warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)
pd.set_option('display.float_format', '{:.2f}'.format)

# Zepto Chemicals ZID 
ZID = os.getenv('ZID_ZEPTO_CHEMICALS')

def time_delta (days):
    today_date = datetime.today()
    delta_day = today_date - timedelta(days=days)
    delta_day = delta_day.strftime("%Y-%m-%d")
    return delta_day


# %%
def avg_rate (ZID, time_delta):
    query = """SELECT
        opodt.xitem, 
        caitem.xdesc,
        SUM(opodt.xqtyord) as qty,
        sum(opodt.xlineamt) as TotalSales,
    ( sum(opodt.xlineamt) / SUM(opodt.xqtyord) ) as avgprice,
    caitem.xstdprice as caitemprice
    FROM
        opord
        LEFT JOIN opodt ON opord.xordernum = opodt.xordernum
        LEFT JOIN caitem ON opodt.xitem = caitem.xitem
    WHERE
        opord.zid = {}
        AND opodt.zid = {}
        AND caitem.zid = {}
        AND  opord.xdate >= '{}'
    AND opord.xstatusord = '5-Delivered'


    GROUP BY
        opodt.xitem, caitem.xdesc, caitem.xstdprice
    ORDER BY
        opodt.xitem
        
    """.format(ZID, ZID, ZID, time_delta )
    df = pd.read_sql(query, con = engine)
    return df

# %%
df_avg_1 = avg_rate (ZID, time_delta(30))
df_avg_2 = avg_rate (ZID, time_delta(15))
df_avg_3 = avg_rate (ZID, time_delta(7))

# %%
df_avg_1 = avg_rate (ZID, time_delta(30))
df_avg_1.columns.values[4] = 'last_30_days_avg'

df_avg_2 = df_avg_2.loc[: , ['xitem','avgprice']].rename(columns={'avgprice': 'last_15_days_avg'})
df_avg_3 = df_avg_3.loc[: , ['xitem','avgprice']].rename(columns={'avgprice': 'last_7_days_avg'})

df_avg_main = pd.merge(df_avg_1, df_avg_2, on='xitem', how='left')

# Merge the result with df_avg_3 on the 'xitem' column
df_avg_main = pd.merge(df_avg_main, df_avg_3, on='xitem', how='left')
caitem_col = df_avg_main.pop('caitemprice')
df_avg_main

# %%
df_avg_main.insert(7 , 'present_price' , caitem_col)
df_avg_main

# %%
with pd.ExcelWriter("HM_07_ZeptoItemPrice.xlsx" ) as writer:
    df_avg_main.to_excel(writer, sheet_name='ItemPrice')

# === Email ===

try:
    # Extract report name from filename
    report_name = os.path.splitext(os.path.basename(__file__))[0]
    recipients = get_email_recipients(report_name)
    print(f"📬 Recipients: {recipients}")
    # recipients = ["ithmbrbd@gmail.com"]  # sample email for demonstration, replace with actual recipients in the actual script.
except Exception as e:
    print(f"⚠️ Failed to fetch recipients: {e}")
    recipients = ["ithmbrbd@gmail.com"]  # Fallback

HTML_BODY_TEXT = """
    <h4>Dear Sir,</h4>
    <p>Please find the attached Excel file containing the <strong> <code>ZEPTO product average price </code></strong> for the last 30 days.</p>

    <p>Best regards,</p>
    <code>Automated Reporting System <b>(pythonhmbr)</b></code>
"""

send_mail(
    subject=f"HM_07 Zepto Monthly Product Average Price Last 30 Days From {time_delta(30)}",
    bodyText= HTML_BODY_TEXT,
    attachment=['HM_07_ZeptoItemPrice.xlsx'],
    recipient=recipients,
    html_body = [(df_avg_main, f'ITEM AVERAGE PRICE FROM {time_delta(30)} ')] #optional if any
)
print("📧 Email sent successfully")


# === Cleanup ===
engine.dispose()
print("✅ Process completed")

