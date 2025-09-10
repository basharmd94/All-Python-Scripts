"""
📊 HM_18_Salesman_Wise_HMBR_Zepto_Comparison.py – Daily & Monthly Sales Comparison

🚀 PURPOSE:
    - Analyze salesman-wise sales in HMBR and Zepto
    - Track: Total Sale, Home Product Sale, Zepto Product Sale, Returns
    - Generate bar charts (daily & monthly)
    - Export to Excel + send email with images & summary

🏢 AFFECTED BUSINESSES:
    - HMBR (Gulshan Trading, ZID=100001)
    - Zepto Chemicals (ZID=100005)

📅 PERIOD:
    - Daily: 2 days before today (e.g., if today is 5th, report for 3rd)
    - Monthly: From 1st of current month to 2 days before today

📁 OUTPUT:
    - HM_18_Salesman_Wise_HMBR_Zepto_Comparison.xlsx (2 sheets: Daily, Monthly)
    - one_day.png, one_month.png (bar charts)
    - Email with embedded charts + attachment

📬 EMAIL:
    - Recipients: get_email_recipients("HM_18_Salesman_Wise_HMBR_Zepto_Comparison")
    - Fallback: ithmbrbd@gmail.com
    - Subject: "Salesman wise cumulative sale HMBR and Zepto"
    - Body: "Dear Sir,"

🎨 VISUALIZATION:
    - Bar chart: 4 stacks per salesman
        - Sale without return
        - Home product sale
        - Zepto product sale
        - Return amount
"""

import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import matplotlib.pyplot as plt


# ─────────────────────────────────────────────────────────────────────
# 🌍 1. Load Environment & Setup
# ─────────────────────────────────────────────────────────────────────
load_dotenv()

print("🌍 Loading configuration...")

# Load ZIDs from .env
try:
    ZID_HMBR = int(os.environ["ZID_GULSHAN_TRADING"])  # 100001
    ZID_ZEPTO = int(os.environ["ZID_ZEPTO_CHEMICALS"])  # 100005
except KeyError as e:
    raise RuntimeError(f"❌ Missing ZID in .env: {e}")

# Date Setup
def get_date(days_offset=2, day_override=None):
    """Get formatted date with optional day override."""
    now = datetime.now() - timedelta(days=days_offset)
    if day_override:
        now = now.replace(day=int(day_override))
    return now.strftime("%Y-%m-%d")

from_date = get_date(2)  # 2 days ago
to_date = get_date(2)    # same as from (single day)
from_first_day_of_month = get_date(30, "01")  # from 1st of month
frm_return_date = get_date(1)
to_return_date = get_date(1)

print(f"📅 Daily Report: {from_date}")
print(f"📅 Monthly Report: {from_first_day_of_month} → {to_date}")


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
# 📥 4. Fetch Sales Data
# ─────────────────────────────────────────────────────────────────────
def get_sales(zid, frm_date, to_date):
    """Fetch sales by salesman and item group."""
    query = text("""
        SELECT 
            opord.xsp, 
            prmst.xname,
            caitem.xitem,
            caitem.xgitem,
            SUM(opodt.xdtwotax) AS xdtwotax
        FROM opord
        JOIN opodt ON opord.xordernum = opodt.xordernum
        JOIN caitem ON opodt.xitem = caitem.xitem
        JOIN prmst ON opord.xsp = prmst.xemp
        WHERE opord.zid = :zid
          AND opodt.zid = :zid
          AND caitem.zid = :zid
          AND prmst.zid = :zid
          AND opord.xdate BETWEEN :frm_date AND :to_date
        GROUP BY opord.xsp, prmst.xname, caitem.xitem, caitem.xgitem
        ORDER BY opord.xsp
    """)
    return pd.read_sql(query, engine, params={'zid': zid, 'frm_date': frm_date, 'to_date': to_date})


# ─────────────────────────────────────────────────────────────────────
# 📤 5. Fetch Returns (SR + RECA)
# ─────────────────────────────────────────────────────────────────────
def get_return(zid, frm_date, to_date):
    """Fetch return from opcrn (SR)."""
    query = text("""
        SELECT opcrn.xemp, SUM(opcdt.xlineamt) AS sr_sum
        FROM opcrn
        JOIN opcdt ON opcrn.xcrnnum = opcdt.xcrnnum
        WHERE opcrn.zid = :zid
          AND opcdt.zid = :zid
          AND opcrn.xdate BETWEEN :frm_date AND :to_date
        GROUP BY opcrn.xemp
    """)
    df = pd.read_sql(query, engine, params={'zid': zid, 'frm_date': frm_date, 'to_date': to_date})
    return df[['xemp', 'sr_sum']] if not df.empty else pd.DataFrame(columns=['xemp', 'sr_sum'])

def get_return_reca(zid, frm_date, to_date):
    """Fetch return from imtemptrn (RECA)."""
    query = text("""
        SELECT imtemptrn.xemp, SUM(imtemptdt.xlineamt) AS reca_sum
        FROM imtemptrn
        JOIN imtemptdt ON imtemptrn.ximtmptrn = imtemptdt.ximtmptrn
        WHERE imtemptrn.zid = :zid
          AND imtemptdt.zid = :zid
          AND imtemptrn.xdate BETWEEN :frm_date AND :to_date
          AND imtemptrn.ximtmptrn LIKE '%RECA%'
        GROUP BY imtemptrn.xemp
    """)
    df = pd.read_sql(query, engine, params={'zid': zid, 'frm_date': frm_date, 'to_date': to_date})
    return df[['xemp', 'reca_sum']] if not df.empty else pd.DataFrame(columns=['xemp', 'reca_sum'])


# ─────────────────────────────────────────────────────────────────────
# 🧮 6. Process HMBR Sales
# ─────────────────────────────────────────────────────────────────────
def process_hmbr_sales(zid, frm_date, to_date):
    """Process HMBR sales with guaranteed columns."""
    df = get_sales(zid, frm_date, to_date)
    
    # Define expected columns
    columns = ['xsp', 'xname', 'gross_sale', 'home_product_sale', 'zepto_product_in_hmbr']
    
    if df.empty:
        return pd.DataFrame({col: [] for col in columns})

    # Total sale
    total = df.groupby(['xsp', 'xname'])['xdtwotax'].sum().reset_index()
    total = total.rename(columns={'xdtwotax': 'gross_sale'})

    # Home product sale
    home = df[df['xgitem'] == 'Industrial & Household']
    home = home.groupby(['xsp'])['xdtwotax'].sum().reset_index()
    home = home.rename(columns={'xdtwotax': 'home_product_sale'})

    # Zepto product sale (FZ items)
    zepto_in_hmbr = df[(df['xgitem'] == 'Industrial & Household') & (df['xitem'].str.contains('FZ', na=False))]
    zepto_in_hmbr = zepto_in_hmbr.groupby(['xsp'])['xdtwotax'].sum().reset_index()
    zepto_in_hmbr = zepto_in_hmbr.rename(columns={'xdtwotax': 'zepto_product_in_hmbr'})

    # Merge
    result = pd.merge(total, home, on='xsp', how='left')
    result = pd.merge(result, zepto_in_hmbr, on='xsp', how='left')
    result = result.fillna(0)

    # Ensure all columns exist
    for col in ['gross_sale', 'home_product_sale', 'zepto_product_in_hmbr']:
        if col not in result.columns:
            result[col] = 0.0

    return result[columns]  # Return in fixed order


# Daily & Monthly HMBR
daily_hmbr = process_hmbr_sales(ZID_HMBR, from_date, to_date)
monthly_hmbr = process_hmbr_sales(ZID_HMBR, from_first_day_of_month, to_date)


# ─────────────────────────────────────────────────────────────────────
# 🧮 7. Process Returns (HMBR & Zepto)
# ─────────────────────────────────────────────────────────────────────
def get_total_return(zid, frm_date, to_date):
    """Fetch total return (SR + RECA) with guaranteed columns."""
    df_sr = get_return(zid, frm_date, to_date)
    df_reca = get_return_reca(zid, frm_date, to_date)

    if df_sr.empty and df_reca.empty:
        return pd.DataFrame({'xemp': [], 'sum_of_return': []})

    df = pd.merge(df_sr, df_reca, on='xemp', how='outer').fillna(0)
    df['sum_of_return'] = df['sr_sum'] + df['reca_sum']
    return df[['xemp', 'sum_of_return']]


# Daily & Monthly Returns
daily_return_hmbr = get_total_return(ZID_HMBR, frm_return_date, to_return_date)
monthly_return_hmbr = get_total_return(ZID_HMBR, from_first_day_of_month, to_return_date)

daily_return_zepto = get_total_return(ZID_ZEPTO, frm_return_date, to_return_date)
monthly_return_zepto = get_total_return(ZID_ZEPTO, from_first_day_of_month, to_return_date)


# ─────────────────────────────────────────────────────────────────────
# 🧮 8. Process Zepto Sales
# ─────────────────────────────────────────────────────────────────────
def process_zepto_sales(zid, frm_date, to_date):
    """Process Zepto sales with guaranteed columns."""
    df = get_sales(zid, frm_date, to_date)
    if df.empty:
        return pd.DataFrame({'xsp': [], 'xname': [], 'gross_sale_zepto': []})
    result = df.groupby(['xsp', 'xname'])['xdtwotax'].sum().reset_index()
    result = result.rename(columns={'xdtwotax': 'gross_sale_zepto'})
    return result[['xsp', 'xname', 'gross_sale_zepto']]


daily_zepto = process_zepto_sales(ZID_ZEPTO, from_date, to_date)
monthly_zepto = process_zepto_sales(ZID_ZEPTO, from_first_day_of_month, to_date)


# ─────────────────────────────────────────────────────────────────────
# 🔗 9. Merge HMBR + Zepto
# ─────────────────────────────────────────────────────────────────────
def merge_sales_returns_hmbr(df_sale, df_return):
    """Merge sales and return data safely."""
    if df_sale.empty:
        return pd.DataFrame({
            'xsp': [],
            'xname': [],
            'gross_sale': [],
            'home_product_sale': [],
            'zepto_product_in_hmbr': [],
            'total_return_hmbr': []
        })
    
    df = pd.merge(df_sale, df_return, left_on='xsp', right_on='xemp', how='left')
    df = df.drop(columns=['xemp'], errors='ignore')
    df['total_return_hmbr'] = df['sum_of_return'].fillna(0)
    df = df.drop(columns=['sum_of_return'], errors='ignore')
    return df


def merge_hmbr_zepto(df_hmbr, df_zepto):
    """Merge HMBR and Zepto data on xname."""
    if df_hmbr.empty and df_zepto.empty:
        return pd.DataFrame()
    df = pd.merge(df_hmbr, df_zepto, on='xname', how='left')
    df = df.fillna(0)
    df = df.rename(columns={
        'xsp_x': 's_id_in_hmbr',
        'xsp_y': 'zepto_sid',
        'sum_of_return_x': 'total_return_hmbr',
        'sum_of_return_y': 'sum_of_return_zepto'
    })
    return df


# Daily
daily_hmbr_w_ret = merge_sales_returns_hmbr(daily_hmbr, daily_return_hmbr)
daily_zepto_w_ret = merge_sales_returns_hmbr(daily_zepto, daily_return_zepto)
daily_final = merge_hmbr_zepto(daily_hmbr_w_ret, daily_zepto_w_ret)

# Monthly
monthly_hmbr_w_ret = merge_sales_returns_hmbr(monthly_hmbr, monthly_return_hmbr)
monthly_zepto_w_ret = merge_sales_returns_hmbr(monthly_zepto, monthly_return_zepto)
monthly_final = merge_hmbr_zepto(monthly_hmbr_w_ret, monthly_zepto_w_ret)


# ─────────────────────────────────────────────────────────────────────
# 📊 10. Visualization
# ─────────────────────────────────────────────────────────────────────
def plot_sales_graph(df, title, filename):
    if df.empty:
        print(f"📊 Skipping {filename}: no data")
        return
    plt.figure(figsize=(14, 6))
    plt.bar(df['xname'], df.get('gross_sale', 0), label='Sale without return', color="#54B435")
    plt.bar(df['xname'], df.get('home_product_sale', 0), label='Home Product Sale', color="#0F3460")
    plt.bar(df['xname'], df.get('zepto_product_in_hmbr', 0), label='Zepto in HMBR', color="#E0144C")
    plt.bar(df['xname'], df.get('total_return_hmbr', 0), label='Return', color="#3C4048")
    plt.xlabel("Salesman")
    plt.ylabel("Amount (BDT)")
    plt.title(title, weight='bold')
    plt.legend()
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.savefig(filename, dpi=120, bbox_inches='tight')
    plt.close()
    print(f"✅ Chart saved: {filename}")


plot_sales_graph(daily_final, f"Daily Sales – {from_date}", "one_day.png")
plot_sales_graph(monthly_final, f"Monthly Sales – {from_first_day_of_month} to {to_date}", "one_month.png")


# ─────────────────────────────────────────────────────────────────────
# 📁 11. Export to Excel
# ─────────────────────────────────────────────────────────────────────
OUTPUT_FILE = "HM_18_Salesman_Wise_HMBR_Zepto_Comparison.xlsx"

print(f"📁 Writing to {OUTPUT_FILE}...")
with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
    daily_final.to_excel(writer, sheet_name='Daily', index=False)
    monthly_final.to_excel(writer, sheet_name='Monthly', index=False)

print(f"✅ Excel saved: {OUTPUT_FILE}")


# ─────────────────────────────────────────────────────────────────────
# 📬 12. Send Email
# ─────────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────
# 📬 12. Send Email (Compatible with Current mail.py)
# ─────────────────────────────────────────────────────────────────────
try:
    recipients = get_email_recipients(os.path.splitext(os.path.basename(__file__))[0])
    print(f"📬 Recipients: {recipients}")
except Exception as e:
    print(f"⚠️ Fallback: {e}")
    recipients = ["ithmbrbd@gmail.com"]

subject = "Salesman wise cumulative sale HMBR and Zepto"
body_text = """
Dear Sir,

Please find the daily and monthly sales performance report for HMBR and Zepto.

Includes:
- Salesman-wise total sales
- Home product vs Zepto product sales
- Return amounts
- Bar charts (attached)

Full data in Excel attachment.

Best regards,
Automated Reporting System
"""

# --- Custom Email with Inline Images ---
from pretty_html_table import build_table
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email import encoders
import smtplib


EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
# ─────────────────────────────────────────────────────────────────────
# 📬 12. Send Email – Charts in Body, No Attachment Needed# ─────────────────────────────────────────────────────────────────────
# 📬 12. Send Email – Charts EMBEDDED in Body (Not as Attachment)
# ─────────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────
# 📬 12. Send Email – Guaranteed Inline Charts (Tested on Gmail)
# ─────────────────────────────────────────────────────────────────────
try:
    recipients = get_email_recipients(os.path.splitext(os.path.basename(__file__))[0])
    print(f"📬 Recipients: {recipients}")
except Exception as e:
    print(f"⚠️ Fallback: {e}")
    recipients = ["ithmbrbd@gmail.com"]

subject = "Salesman wise cumulative sale HMBR and Zepto"

# Plain text fallback
body_text = """
Dear Sir,

Please find the daily and monthly sales performance report.

Charts are embedded below. Excel is attached for reference.

Best regards,
Automated Reporting System
"""

# HTML with inline images
html = """
<html>
  <body style="font-family: Arial, sans-serif; color: #333; line-height: 1.6;">
    <p>Dear Sir,</p>
    
    <p>Please find the sales performance report for HMBR and Zepto.</p>

    <h3 style="color: #2c3e50;">📊 Last Day Sales (HMBR + Zepto)</h3>
    <img src="cid:daily_chart" width="100%" style="max-width: 800px; height: auto; border: 1px solid #eee; border-radius: 8px;" />

    <h3 style="color: #2c3e50;">📅 Monthly Sales Summary</h3>
    <img src="cid:monthly_chart" width="100%" style="max-width: 800px; height: auto; border: 1px solid #eee; border-radius: 8px;" />

    <p><em>Note: Generated on {date}</em></p>
    <p>Best regards,<br><strong>System Generated Email</strong></p>
  </body>
</html>
""".format(date=datetime.now().strftime("%Y-%m-%d"))

# --- Build MIME message ---
from email.mime.image import MIMEImage

msg = MIMEMultipart('related')
msg['Subject'] = subject
msg['From'] = EMAIL_USER
msg['To'] = ", ".join(recipients)

# 1. Attach plain text
msg.attach(MIMEText(body_text, 'plain'))

# 2. Attach HTML (must come before images for Gmail)
msg.attach(MIMEText(html, 'html'))

# 3. Attach images as inline (MUST come AFTER HTML)
image_map = [
    ("one_day.png", "daily_chart", "Daily Sales Chart"),
    ("one_month.png", "monthly_chart", "Monthly Sales Chart")
]

for path, cid, name in image_map:
    if os.path.exists(path):
        try:
            with open(path, 'rb') as f:
                img = MIMEImage(f.read())
            img.add_header('Content-ID', f'<{cid}>')  # <-- Critical: <daily_chart>
            img.add_header('Content-Disposition', 'inline', filename=name)
            msg.attach(img)
            print(f"✅ Embedded: {path} as cid:{cid}")
        except Exception as e:
            print(f"❌ Failed to embed {path}: {e}")
    else:
        print(f"❌ Missing image file: {path}")

# 4. Optional: Attach Excel (won't affect inline images)
include_excel = True
if include_excel and os.path.exists(OUTPUT_FILE):
    try:
        with open(OUTPUT_FILE, "rb") as f:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(f.read())
        encoders.encode_base64(part)
        filename = os.path.basename(OUTPUT_FILE)
        part.add_header('Content-Disposition', 'attachment', filename=filename)
        msg.attach(part)
        print(f"📎 Attached: {filename}")
    except Exception as e:
        print(f"❌ Failed to attach Excel: {e}")

# 5. Send email
try:
    server = smtplib.SMTP('smtp.gmail.com:587')
    server.starttls()
    server.login(EMAIL_USER, EMAIL_PASSWORD)
    server.sendmail(EMAIL_USER, recipients, msg.as_string())
    server.quit()
    print("✅ Email sent! Check inbox. Images may be blocked by default.")
except Exception as e:
    print(f"❌ Failed to send email: {e}")