"""
🚀 HM_12_Refresh_Materialized_View.py – Refresh final_items_view

📌 PURPOSE:
    - Refresh the materialized view `final_items_view`
    - Notify team via email when complete
"""

import os
import sys
from sqlalchemy import create_engine
from dotenv import load_dotenv


# === 1. Load Environment Variables from .env ===
load_dotenv()


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


# === 5. Function: Refresh Materialized View ===
def refresh_materialized_view():
    """Refresh the materialized view `final_items_view`."""
    try:
        print("🔄 Refreshing materialized view 'final_items_view'...")
        with engine.begin() as conn:
            # Execute refresh
            conn.execute("REFRESH MATERIALIZED VIEW final_items_view;")
        print("✅ Materialized view refreshed successfully.")
    except Exception as e:
        print(f"❌ Error refreshing materialized view: {e}")
        send_mail(
            subject="HM_12 Sync Failed",
            bodyText=f"Failed to refresh materialized view.\nError: {e}",
            attachment=[],
            recipient=["ithmbrbd@gmail.com"]
        )
        sys.exit(1)


# === 6. Main Execution ===
if __name__ == "__main__":
    print("🚀 HM_12: Sync Started")
    refresh_materialized_view()

    # Send success email
    try:
        recipients = get_email_recipients(os.path.splitext(os.path.basename(__file__))[0])
        print(f"📬 Success email to: {recipients}")
    except Exception as e:
        print(f"⚠️ Fallback: {e}")
        recipients = ["ithmbrbd@gmail.com"]

    send_mail(
        subject="HM_12 Sync Completed",
        bodyText="The materialized view 'final_items_view' has been successfully refreshed.",
        attachment=[],
        recipient=recipients
    )

    print("✅ HM_12: Sync Completed & email sent.")