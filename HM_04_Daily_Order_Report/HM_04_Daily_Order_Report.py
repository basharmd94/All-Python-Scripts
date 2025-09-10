"""
📊 daily_order_report.py – Daily Order Report (HMBR Group)

📌 PURPOSE:
Generates a daily order summary for:
  - HMBR (ZID: 100001)
  - Zepto Chemicals (ZID: 100005)
  - GI Corp / Karigor (ZID: 100000)

📤 OUTPUTS:
  - Excel: dailyOrderReport.xlsx
  - HTML email body (passed directly to send_mail)
  - Email sent via shared mail.send_mail()

🔧 DEPENDS ON:
  - project_config.DATABASE_URL
  - mail.send_mail(), get_email_recipients()
"""

import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine

# === 1. Add project root to Python path ===
CURRENT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# === 2. Import shared modules ===
try:
    from mail import send_mail, get_email_recipients
    from project_config import DATABASE_URL
except ImportError as e:
    print(f"❌ Failed to import shared modules: {e}")
    raise

# === 3. Create database engine ===
engine = create_engine(DATABASE_URL)

# === Configuration ===
COMPANY_ZIDS = {
    'HMBR': 100001,
    'ZEPTO': 100005,
    'GICORP': 100000
}

# Date filter: orders from today onward
TODAY_DATE = datetime.now().strftime('%Y-%m-%d')

# Custom item sort order for HMBR
HM_BR_PRIORITY_ITEMS = [
    "1779", "1780", "1281", "0787", "1280", "1005", "17220",
    "1100", "0788", "01990", "1787", "1176", "11721", "1777"
]


# === Query Function ===
def fetch_order_data(zid: int, start_date: str) -> pd.DataFrame:
    """Fetch order data for a given ZID and date range."""
    query = """
        SELECT 
            opodt.zid,
            opord.xdate,
            opodt.xitem,
            caitem.xdesc,
            SUM(opodt.xqtyord) AS order_qty,
            SUM(opodt.xdtwotax) AS total_value
        FROM opodt
        JOIN opord ON opodt.xordernum = opord.xordernum
        JOIN caitem ON caitem.xitem = opodt.xitem
        WHERE opodt.zid = %s
          AND opord.zid = %s
          AND caitem.zid = %s
          AND opord.xdate >= %s
        GROUP BY opodt.zid, opord.xdate, opodt.xitem, caitem.xdesc
        ORDER BY order_qty DESC
    """
    try:
        return pd.read_sql(query, con=engine, params=(zid, zid, zid, start_date))
    except Exception as e:
        print(f"❌ Query failed for ZID {zid}: {e}")
        return pd.DataFrame()


# === HMBR Custom Sort ===
def sort_hmbr_by_priority(df: pd.DataFrame) -> pd.DataFrame:
    """Sort HMBR items by predefined priority list."""
    if df.empty:
        return df

    df['xitem_cat'] = pd.Categorical(df['xitem'], categories=HM_BR_PRIORITY_ITEMS, ordered=True)
    df = df.sort_values('xitem_cat', kind='stable').reset_index(drop=True)
    df = df.rename(columns={'xitem': 'xitem3'})[['xitem3', 'xdesc', 'order_qty', 'total_value']]
    return df


# === Generate HTML Email Body ===
def generate_html_body(df_hmbr: pd.DataFrame, df_zepto: pd.DataFrame, df_gicorp: pd.DataFrame) -> str:
    """Generate HTML content for email body."""
    return f"""
    <html>
    <body style="font-family: Arial, sans-serif; margin: 20px;">
        <h2 style="color: #2c3e50; border-bottom: 2px solid #3498db;">HMBR</h2>
        {df_hmbr.to_html(classes='dataframe', index=False)}

        <h2 style="color: #2c3e50; border-bottom: 2px solid #3498db; margin-top: 30px;">ZEPTO CHEMICALS</h2>
        {df_zepto.to_html(classes='dataframe', index=False)}

        <h2 style="color: #2c3e50; border-bottom: 2px solid #3498db; margin-top: 30px;">GI CORPORATION</h2>
        {df_gicorp.to_html(classes='dataframe', index=False)}
    </body>
    </html>
    """


# === Main Execution ===
def main():
    print("🚀 Starting Daily Order Report generation...")

    # Fetch data
    df_hmbr = fetch_order_data(COMPANY_ZIDS['HMBR'], TODAY_DATE)
    df_zepto = fetch_order_data(COMPANY_ZIDS['ZEPTO'], TODAY_DATE)
    df_gicorp = fetch_order_data(COMPANY_ZIDS['GICORP'], TODAY_DATE)

    # Apply sorting
    df_hmbr_sorted = sort_hmbr_by_priority(df_hmbr)
    df_zepto_sorted = df_zepto.sort_values('total_value', ascending=False).reset_index(drop=True)
    df_gicorp_sorted = df_gicorp.sort_values('total_value', ascending=False).reset_index(drop=True)

    # =============================
    # Export to Excel
    # =============================
    with pd.ExcelWriter('dailyOrderReport.xlsx', engine='openpyxl') as writer:
        df_hmbr_sorted.to_excel(writer, sheet_name='order_hmbr', index=False)
        df_zepto_sorted.to_excel(writer, sheet_name='order_zepto', index=False)
        df_gicorp_sorted.to_excel(writer, sheet_name='order_gicorp', index=False)
    print("✅ Excel report generated: dailyOrderReport.xlsx")

    # =============================
    # Generate HTML for Email Body
    # =============================
    html_content = generate_html_body(df_hmbr_sorted, df_zepto_sorted, df_gicorp_sorted)

    # =============================
    # Send Email via Shared Module
    # =============================
  # =============================
# Send Email via Shared Module
# =============================
    script_name = Path(__file__).stem

    try:
        recipients = get_email_recipients(script_name)
        print(f"📬 Recipients: {recipients}")
    except Exception as e:
        print(f"⚠️ Fallback: {e}")
        recipients = ["ithmbrbd@gmail.com"]

    # Prepare list of (DataFrame, heading) for send_mail
    html_body = [
        (df_hmbr_sorted, "HMBR"),
        (df_zepto_sorted, "ZEPTO CHEMICALS"),
        (df_gicorp_sorted, "GI CORPORATION")
    ]

    send_mail(
        subject=f"HM_04 Daily Order Report – {TODAY_DATE}",
        bodyText="Please find today's order summary below.",
        html_body=html_body,  # ← Pass list of tuples, NOT raw HTML
        attachment=['dailyOrderReport.xlsx'],
        recipient=recipients
    )
    print("📧 Email sent successfully.")

    # =============================
    # Cleanup
    # =============================
    engine.dispose()
    print("✅ Script completed. Database engine disposed.")


if __name__ == "__main__":
    main()