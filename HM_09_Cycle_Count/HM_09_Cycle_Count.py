"""
🚀 HM_09_Cycle_Count_Notification.py – Daily Cycle Count Assignment

📌 PURPOSE:
    - Select 3 random zids from fixed list: GI, Gulshan Trading, Zepto, Grocery, Packaging
    - For each: pick 3 high-value items (value-weighted)
    - Log in quarterly JSON
    - Send single HTML email to fixed counter list
"""

import os
import sys
import json
import random
from datetime import date
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text


# === 1. Load Environment Variables from .env ===
load_dotenv()



# === 2. Add root (E:\) to Python path ===
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


# === 3. Import shared modules ===
from mail import send_mail, get_email_recipients
# Use shared DATABASE_URL
from project_config import DATABASE_URL

# === 4. Fixed Counter Email List (from .env) ===
try:
    COUNTER_EMAILS = os.getenv("COUNTER_EMAILS", "").replace(" ", "").split(",")
    COUNTER_EMAILS = [email for email in COUNTER_EMAILS if email]
    if not COUNTER_EMAILS:
        raise ValueError("COUNTER_EMAILS is empty or missing")
except Exception as e:
    raise RuntimeError(f"❌ Invalid COUNTER_EMAILS in .env: {e}")

print(f"📧 Cycle count emails will be sent to: {COUNTER_EMAILS}")


# === 5. Fixed ZIDs and Business Names (Only These 5) ===
ALL_ZIDS = [100000, 100001, 100005, 100006, 100009]

DEPT = {
    100000: "GI",
    100001: "Gulshan Trading",
    100005: "Zepto",
    100006: "Grocery Shop",
    100009: "Packaging",
}

# Business Name mapping (same as DEPT in this case, but can be extended)
BUSINESS_NAME = {
    100000: "GI Corporation.",
    100001: "GULSHAN TRADING",
    100005: "Zepto Chemicals",
    100006: "HMBR Grocery Shop",
    100009: "Gulshan Packaging",
}


# === 6. Constants ===
LOG_DIR = "count_logs"
POOL_N = 200                  # top-N by stockvalue before sampling
ITEMS_PER_ZID = 3
ZIDS_PER_DAY = 3
SKIP_DAYS = {"Friday"}        # Skip email on these days


# === 7. Quarter Helpers / JSON Logs ===
def quarter_start(d: date) -> str:
    """Return first day of the quarter as YYYY-MM-DD."""
    m = (d.month - 1) // 3 * 3 + 1
    return date(d.year, m, 1).isoformat()

def log_path(zid: int, qroot: str) -> str:
    """Return path to JSON log for zid."""
    return os.path.join(qroot, f"{zid}.json")

def load_counted(zid: int, qroot: str) -> set:
    """Load set of already counted itemcodes for this zid this quarter."""
    fp = log_path(zid, qroot)
    if os.path.exists(fp):
        with open(fp, 'r') as f:
            return set(json.load(f))
    return set()

def append_counted(zid: int, itemcodes: list, qroot: str):
    """Append new itemcodes to zid's quarterly log."""
    fp = log_path(zid, qroot)
    counted = load_counted(zid, qroot)
    counted.update(itemcodes)
    os.makedirs(qroot, exist_ok=True)
    with open(fp, "w") as f:
        json.dump(sorted(counted), f, indent=2)


# === 8. Data Pull: Inventory Stock Value ===
SQL = """
SELECT 
    imtrn.zid,
    imtrn.xitem AS itemcode,
    caitem.xdesc AS itemname,
    SUM(imtrn.xqty * imtrn.xsign) AS stockqty,
    SUM(imtrn.xval * imtrn.xsign) AS stockvalue
FROM imtrn
JOIN caitem ON imtrn.xitem = caitem.xitem AND imtrn.zid = caitem.zid
WHERE imtrn.zid = :zid
GROUP BY imtrn.zid, imtrn.xitem, caitem.xdesc
"""

def pull_inventory_for_all(zids: list[int], engine) -> dict[int, pd.DataFrame]:
    """Fetch inventory data for all zids."""
    out = {}
    with engine.begin() as conn:
        for zid in zids:
            df = pd.read_sql(text(SQL), conn, params={"zid": zid})
            out[zid] = df.dropna(subset=['itemcode'])
    return out


# === 9. Selection Logic ===
def remaining_pool(df: pd.DataFrame, counted: set) -> pd.DataFrame:
    """Filter out already counted; keep top-N by stockvalue."""
    if df.empty:
        return df
    rem = df[~df["itemcode"].isin(counted)]
    if rem.empty:
        return pd.DataFrame()
    return rem.sort_values("stockvalue", ascending=False).head(POOL_N).copy()

def choose_zids_uniform(eligible_zids: list[int]) -> list[int]:
    """Randomly select up to ZIDS_PER_DAY distinct zids."""
    if not eligible_zids:
        return []
    k = min(ZIDS_PER_DAY, len(eligible_zids))
    return random.sample(eligible_zids, k=k)

def choose_items_value_weighted(pool_df: pd.DataFrame, n: int) -> pd.DataFrame:
    """Sample items weighted by stockvalue (fallback to uniform)."""
    if pool_df.empty:
        return pd.DataFrame()
    weights = pool_df["stockvalue"].clip(lower=0)
    total_weight = weights.fillna(0).sum()
    if total_weight <= 0:
        return pool_df.sample(n=min(n, len(pool_df)), replace=False)
    return pool_df.sample(n=min(n, len(pool_df)), weights=weights, replace=False)

# === 10. Build Final DataFrame & HTML Email ===
def build_html(today: str, df: pd.DataFrame) -> str:
    """Generate HTML body from DataFrame with Business Name."""
    if df.empty:
        return f"""
        <p>No fresh items remain for any selected department today ({today}).</p>
        """

    # Add columns in order
    df['Department'] = df['zid'].map(DEPT)
    df['Business Name'] = df['zid'].map(BUSINESS_NAME)

    # Reorder columns
    df = df[["Department", "zid", "Business Name", "itemcode", "itemname"]]

    intro = f"""
    <p>Please perform a blind count of the following items today ({today}).</p>
    """
    table = df.to_html(index=False, border=0, justify="left", table_id="cycle-count")
    return intro + table


# === 11. Main Logic ===
def main():
    today = date.today()
    weekday = today.strftime("%A")

    if weekday in SKIP_DAYS:
        print(f"📅 {weekday}: Skipping cycle count email.")
        return

    # Quarter directory
    q_start = quarter_start(today)
    q_root = os.path.join(LOG_DIR, q_start)
    os.makedirs(q_root, exist_ok=True)

    # Create engine
    engine = create_engine(DATABASE_URL)

    # Load inventory and history
    inv_by_zid = pull_inventory_for_all(ALL_ZIDS, engine)
    counted_by_zid = {z: load_counted(z, q_root) for z in ALL_ZIDS}

    # Build pools and find eligible zids
    pools = {}
    eligible_zids = []
    for zid in ALL_ZIDS:
        pool = remaining_pool(inv_by_zid.get(zid, pd.DataFrame()), counted_by_zid[zid])
        pools[zid] = pool
        if not pool.empty:
            eligible_zids.append(zid)

    # Select zids and items
    chosen_zids = choose_zids_uniform(eligible_zids)
    all_rows = []

    for zid in chosen_zids:
        picks_df = choose_items_value_weighted(pools[zid], ITEMS_PER_ZID)
        if picks_df.empty:
            continue
        picked_items = picks_df[["itemcode", "itemname"]].to_dict("records")
        append_counted(zid, [r["itemcode"] for r in picked_items], q_root)
        for item in picked_items:
            all_rows.append({"zid": zid, "itemcode": item["itemcode"], "itemname": item["itemname"]})

    # Create DataFrame and add Business Name
    df_display = pd.DataFrame(all_rows)
    if not df_display.empty:
        df_display['Department'] = df_display['zid'].map(DEPT)
        df_display['Business Name'] = df_display['zid'].map(BUSINESS_NAME)
        df_display = df_display[["Department", "zid", "Business Name", "itemcode", "itemname"]]
    else:
        df_display = pd.DataFrame(columns=["Department", "zid", "Business Name", "itemcode", "itemname"])

    # Build HTML
    today_str = today.isoformat()
    html_body = build_html(today_str, df_display)
    subject = f"HM_09 Cycle Count – {today_str}"

    # Use dynamic recipient lookup or fallback
    try:
        recipients = get_email_recipients(os.path.splitext(os.path.basename(__file__))[0])
        print(f"📬 Recipients from config: {recipients}")
    except Exception as e:
        print(f"⚠️ Fallback to COUNTER_EMAILS: {e}")
        recipients = ["ithmbrbd@gmail.com"]

    # ✅ Send email with HTML only (no html_body DataFrame)
    send_mail(
        subject=subject,
        bodyText="",  # Will be replaced by HTML
        attachment=[],
        recipient=recipients,
        html_body=html_body  # ← Pass full HTML string
    )

    print(f"✅ {today_str}: Sent {len(all_rows)} items across {len(chosen_zids)} departments.")

# === 12. Run ===
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ ERROR: {e}", file=sys.stderr)
        sys.exit(1)