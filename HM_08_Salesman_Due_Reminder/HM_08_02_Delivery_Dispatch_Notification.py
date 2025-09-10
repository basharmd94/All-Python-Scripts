"""
🚀 HM_08_02_Delivery_Dispatch_Notification.py – Daily Dispatch Alert to Salesmen

📌 PURPOSE:
    - Notify salesmen of today’s dispatched deliveries
    - Include customer balance if >500 BDT
    - Send personalized Excel + HTML email
    - Send summaries to management and director
"""

import os
import sys
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text  # ← Use text() for raw SQL with IN clause
from dotenv import load_dotenv


# === 1. Load Environment Variables from .env ===
load_dotenv()

try:
    ZID = int(os.environ["ZID_GULSHAN_TRADING"])
except KeyError:
    raise RuntimeError("❌ ZID_GULSHAN_TRADING not found in .env")
PROJECT_NAME = "GULSHAN TRADING"

print(f"📌 Processing dispatch for: {PROJECT_NAME} (ZID={ZID})")


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
pd.options.mode.chained_assignment = None


# === 6. Date Setup ===
TODAY = datetime.now().strftime("%Y-%m-%d")
print(f"📅 Processing deliveries for: {TODAY}")


# === 7. Salesman Info (Email + Name) ===
salesman_info_dict = {
    'SA--000068': ('mssalim5904@gmail.com', 'Md. Salimullah (Salim)'),
    'SA--000224': ('aponmdarifulislam603@gmail.com', 'Md. Ariful Islam-2'),
    'SA--000038': ('hbelal706@gmail.com', 'Md. Belal Hossain'),
    'SA--000144': ('hossainmobarok45@gmail.com', 'Syed Mobarok Hossain'),
    'SA--000021': ('mdm10413@gmail.com', 'Md. Maruful Islam'),
    'SA--000193': ('limonmridha30@gmail.com', 'Limon Mridha'),
    'SA--000114': ('plabonpavel494@gmail.com', 'Md. Pavel Mia'),
    'SA--000011': ('jamal.hmbr@gmail.com', 'Jamal Hossain Titu'),
    'SA--000192': ('sajibhossen701@gmail.com', 'Sojib Hossen'),
    'SA--000098': ('javedfeni127@gmail.com', 'Md. Belayet Hossen'),
    'SA--000227': ('mdsumonhossionmithu00@gmail.com', 'Md. Sumon Hossain Mithu'),
    'SA--000242': ('iforhadul08@gmail.com', 'Md. Forhadul Islam')
}

salesman_ids = tuple(salesman_info_dict.keys())


# === 8. Fetch Today's Dispatched DOs ===
query_do = text("""
    SELECT 
        opdor.xdornum AS do_number,
        opdor.xdate AS dodate,
        opdor.xcus,
        CONCAT(opdor.xcus, '--', cacus.xshort) AS customer,
        cacus.xadd1 AS address,
        cacus.xtaxnum AS mobile_number,
        cacus.xcity,
        opdor.xsp,
        opdor.xtotamt AS total_amt,
        opdor.xdatestor AS dispatchdate
    FROM opdor
    LEFT JOIN cacus ON opdor.xcus = cacus.xcus
    WHERE opdor.zid = :zid
      AND opdor.xdatestor = :today
      AND opdor.xflagdor = 'Goods Delivered'
      AND opdor.xsp IN :salesman
    ORDER BY opdor.xdornum
""")

try:
    df_do = pd.read_sql(query_do, engine, params={'zid': ZID, 'today': TODAY, 'salesman': salesman_ids})
except Exception as e:
    print(f"❌ Database query failed: {e}")
    df_do = pd.DataFrame()


# === 9. Check if df_do is valid and has 'xsp' column ===
if df_do.empty:
    print("📭 No deliveries found for today.")
else:
    if 'xsp' not in df_do.columns:
        print("❌ 'xsp' column not found in query result. Check SQL.")
        df_do = pd.DataFrame()
    else:
        df_do['customer_receive_date'] = ""  # Placeholder for feedback


# === 10. Fetch Customer Balance (>500 BDT) ===
if not df_do.empty:
    customer_ids = tuple(df_do['xcus'].unique())
    if len(customer_ids) == 1:
        customer_ids = f"('{customer_ids[0]}')"

    query_balance = f"""
        SELECT 
            gldetail.xsub AS xcus, 
            SUM(gldetail.xprime) AS balance_till_today
        FROM glheader
        JOIN gldetail ON glheader.xvoucher = gldetail.xvoucher
        WHERE glheader.zid = %(zid)s
          AND gldetail.zid = %(zid)s
          AND gldetail.xproj = %(project)s
          AND gldetail.xvoucher NOT LIKE '%%OB%%'
          AND glheader.xdate <= %(today)s
          AND gldetail.xsub IN {customer_ids}
        GROUP BY gldetail.xsub
        HAVING SUM(gldetail.xprime) > 500
    """
    try:
        df_balance = pd.read_sql(query_balance, engine, params={'zid': ZID, 'project': PROJECT_NAME, 'today': TODAY})
        df_do = pd.merge(df_do, df_balance, on='xcus', how='left')
        df_do['balance_till_today'] = df_do['balance_till_today'].fillna(0)
    except Exception as e:
        print(f"❌ Balance query failed: {e}")


# === 11. Send Personalized Emails to Salesmen ===
if not df_do.empty:
    for sp_id, (email, name) in salesman_info_dict.items():
        sp_df = df_do[df_do['xsp'] == sp_id].copy()
        if not sp_df.empty:
            file_path = f"HM_08_02_{sp_id}_dispatch_{TODAY}.xlsx"
            sp_df.to_excel(file_path, index=False)

            html_body = [(sp_df, f"On the Way To Delivery for {name} (Dispatch: {TODAY})")]

            send_mail(
                subject=f"HM_08.2 On the Way to Delivery – {name}",
                bodyText=f"""
                জনাব {name},<br><br>
                অনুগ্রহ করে আজকের ({TODAY}) ডেলিভারির এক্সেল ফাইলটি দেখুন।<br>
                যখন আপনি পণ্য গ্রহণ করবেন, তখন <b>customer_receive_date</b> কলামটি পূরণ করে<br>
                ফাইলটি <b>sohelsorkar356648@gmail.com</b> এবং <b>ithmbrbd@gmail.com</b>-এ পাঠাবেন।
                """,
                attachment=[file_path],
                recipient=[email, 'ithmbrbd@gmail.com'],
                html_body=html_body
            )
            print(f"📨 Sent dispatch list to {name}")
else:
    print("📭 No data to process for salesmen.")


# === 12. Send Summary to Management ===
try:
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    recipients = get_email_recipients(script_name)
except Exception:
    recipients = ['saleshmbrbd@gmail.com', 'ithmbrbd@gmail.com']

if not df_do.empty:
    summary_file = f"HM_08_02_summary_dispatch_{TODAY}.xlsx"
    df_summary = df_do.drop(columns=['xsp', 'xcus'], errors='ignore')
    df_summary.to_excel(summary_file, index=False)

    send_mail(
        subject=f"HM_08.2 Summary – Today's Deliveries ({TODAY})",
        bodyText=f"Daily dispatch summary for {TODAY}. See attached Excel.",
        attachment=[summary_file],
        recipient=recipients,
        html_body=[(df_summary, "On the Way To Delivery")]
    )
else:
    print("📭 No summary to send.")


# === 13. Send to Director (With Balance) ===
if not df_do.empty:
    director_file = f"HM_08_02_director_with_balance_{TODAY}.xlsx"
    df_director = df_do.drop(columns=['xsp', 'xcus'], errors='ignore')
    df_director.to_excel(director_file, index=False)

    send_mail(
        subject=f"HM_08.2 Director Copy – Deliveries with Balance ({TODAY})",
        bodyText=f"Today's delivery list with customer balance >500 BDT.",
        attachment=[director_file],
        recipient=['asaddat87@gmail.com', 'ithmbrbd@gmail.com'],
        html_body=[(df_director, "Deliveries with High Balance Customers")]
    )


# === 14. Cleanup Temp Files ===
import glob
for file in glob.glob("HM_08_02_*_dispatch_*.xlsx"):
    try:
        os.remove(file)
        print(f"🗑️ Deleted: {file}")
    except Exception as e:
        print(f"❌ Failed to delete {file}: {e}")


# === 15. Cleanup ===
engine.dispose()
print("✅ HM_08.2 completed.")