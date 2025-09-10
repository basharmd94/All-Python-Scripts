"""
cycle_counter_email_single.py — run once daily at 09:00

• One designated person handles ALL zids.
• Daily Finished Goods (FG): choose 3 distinct zids uniformly from ALL_ZIDS (with eligible FG items).
  For each: pick 3 items (top-N, quarter exclusion, value-weighted), filtered by FG categories & zid-specific FG warehouses.
• Scheduled Raw/Packaging (RAW): on certain day-of-month per zid, pick 10 items from the configured RAW group,
  filtered by zid-specific RAW warehouses, with a rolling 90-day exclusion.
• HTML email only (no attachments), two sections: FG (Daily) and RAW (Monthly, if scheduled).
"""

import json
import os
import random
import smtplib
import sys
from datetime import date, datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Union

import pandas as pd
from sqlalchemy import create_engine, text

# ────────────────────────────────────────────────────────────────────
# CONFIG
# ────────────────────────────────────────────────────────────────────
DB_URL   = "postgresql://postgres:postgres@localhost:5432/da"
LOG_DIR  = "count_logs"          # JSON history root
POOL_N   = 200                   # top-N by stockvalue before sampling
ITEMS_PER_ZID = 5                # FG: items per chosen zid
ZIDS_PER_DAY  = 3                # FG: zids per day
RAW_ITEMS_PER_DAY = 10           # RAW: items per scheduled group/day
SKIP_DAYS = {"Friday"}           # set() to include Fridays

# One person for all businesses, every day
COUNTER_NAME = "Inventory Controller"
COUNTER_EMAILS = ["asaddat87@gmail.com","admhmbr@gmail.com","financehmbr1@gmail.com","ithmbrbd@gmail.com"]   # ← put the real recipient(s)
# COUNTER_EMAILS = ["ithmbrbd@gmail.com"]   # ← put the real recipient(s)

# Global list of all departments (zids)
ALL_ZIDS = [100000, 100001, 100005, 100009]

DEPT = {
    100000: "GI",
    100001: "Gulshan Trading",
    100005: "Zepto",
    100009: "Packaging"
}

# Category lists by zid (extend as needed)
RAW_GROUPS_BY_ZID = {
    100000: ["RAW Material PL", "RAW Material PR", "RAW Material TH","RAW Material ST", "RAW Material CH","Packaging Item CH", "Packaging Item PL"],
    100005: ["Zepto Raw Metrial","Packaging Item"],
    100009: ["RAW Material Packaging","Import Item"]
    # Add RAW groups for other zids when applicable
}
FG_GROUPS_BY_ZID = {
    100000: ["Thread Tape Item", "Import Item", "Chemical Item","Plastic Item", "Paint Roller Item", "Manufacturing Item", "Steel Item"],
    # If None: don't explicitly whitelist FG groups; instead we exclude RAW groups for that zid.
    100001: ["Household Product", "Industrial & Household", "Furniture Fittings", "Sanitary"],
    100005: ["Industrial & Household"],
    100009: ["Finished Goods Packaging"]
}

# ── NEW: zid-specific allowed warehouses (single dict per flow) ──────
# FG: None means "allow any warehouse" for that zid.
FG_WAREHOUSES_BY_ZID = {
    100000: ["Finished Goods Store", "Sales Warehouse GI", "Manufacturing Store"],
    100001: ["HMBR -W7 (MirerBaazar 3rd Floor)", "HMBR -W5 (MirerBaazar 2nd Floor)", "HMBR -Main Store (4th Floor)", "HMBR -W7 (2) (MirerBaazar 3rd Floor)"],  # FG in multiple warehouses → no restriction
    100005: ["Sales Warehouse(Zepto)", "Finished Goods Warehouse Zepto"],
    100009: ["Finished Goods Store Packaging"]
}
# RAW: provide explicit lists; empty list effectively allows none (not used if not scheduled)
RAW_WAREHOUSES_BY_ZID = {
    100000: ["Raw Material Store", "Manufacturing Store"],
    100001: [],  # no raw materials for 100001
    100005: ["Raw Metrial Warehouse Zepto"],
    100009: ["Raw Material Store Packaging"]
}

# Monthly schedule: day-of-month → RAW itemgroup for a zid
# Extendable to multiple zids if needed: {zid: {day: "Group", ...}, ...}
MONTHLY_RAW_SCHEDULE = {
    100000: {
        21: "RAW Material PR",
        22: "RAW Material TH",
        23: "RAW Material ST",
        24: "RAW Material CH",
        25: "RAW Material PL",
        26: "Packaging Item CH",
        27: "Packaging Item PL",
    },
    100005: {
        19: "Zepto Raw Metrial",
        20: "Packaging Item",
    },
    100009: {
        18: "RAW Material Packaging",
        19: "Import Item",
    }
}

SMTP = dict(host="smtp.gmail.com", port=587,
            user="pythonhmbr12@gmail.com", pwd="vksikttussvnbqef") #update password here

# ────────────────────────────────────────────────────────────────────
# SQL (include itemgroup & warehouse in GROUP BY!)
# ────────────────────────────────────────────────────────────────────
SQL = """
SELECT imtrn.zid,
       imtrn.xitem          AS itemcode,
       caitem.xdesc         AS itemname,
       caitem.xgitem        AS itemgroup,
       imtrn.xwh            AS warehouse,
       SUM(imtrn.xqty * imtrn.xsign) AS stockqty,
       SUM(imtrn.xval * imtrn.xsign) AS stockvalue
FROM   imtrn
JOIN   caitem ON imtrn.xitem = caitem.xitem AND imtrn.zid = caitem.zid
WHERE  imtrn.zid = :zid
GROUP  BY imtrn.zid, imtrn.xitem, caitem.xdesc, caitem.xgitem, imtrn.xwh
"""

# ────────────────────────────────────────────────────────────────────
# QUARTER HELPERS / JSON LOGS
# ────────────────────────────────────────────────────────────────────
def quarter_start(d: date) -> str:
    m = (d.month - 1) // 3 * 3 + 1
    return date(d.year, m, 1).isoformat()

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

# FG: quarter-scoped per-zid logs (no repeats within quarter)
def fg_log_path(zid: int, today: date) -> str:
    q_root = os.path.join(LOG_DIR, "quarter", quarter_start(today))
    ensure_dir(q_root)
    return os.path.join(q_root, f"{zid}.json")

def load_fg_counted(zid: int, today: date) -> set[str]:
    fp = fg_log_path(zid, today)
    return set(json.load(open(fp))) if os.path.exists(fp) else set()

def append_fg_counted(zid: int, itemcodes: list[str], today: date):
    fp = fg_log_path(zid, today)
    counted = load_fg_counted(zid, today)
    counted.update(itemcodes)
    with open(fp, "w") as f:
        json.dump(sorted(counted), f)

# RAW: rolling-90-day per-zid logs (no repeats within 90 days)
def raw_log_path(zid: int) -> str:
    r_root = os.path.join(LOG_DIR, "rolling90")
    ensure_dir(r_root)
    return os.path.join(r_root, f"{zid}.json")

def load_raw_log(zid: int) -> dict:
    fp = raw_log_path(zid)
    return json.load(open(fp)) if os.path.exists(fp) else {}

def save_raw_log(zid: int, mapping: dict):
    fp = raw_log_path(zid)
    with open(fp, "w") as f:
        json.dump(mapping, f, indent=0)

def is_within_90_days(iso_day: str, today: date) -> bool:
    try:
        prev = datetime.fromisoformat(iso_day).date()
    except Exception:
        return False
    return (today - prev) <= timedelta(days=90)

# ────────────────────────────────────────────────────────────────────
# DATA PULL
# ────────────────────────────────────────────────────────────────────
def pull_inventory_for_all(zids: list[int], engine) -> dict[int, pd.DataFrame]:
    out = {}
    with engine.begin() as conn:
        for zid in zids:
            out[zid] = pd.read_sql(text(SQL), conn, params={"zid": zid})
    return out

# ────────────────────────────────────────────────────────────────────
# FILTERS & SELECTION
# ────────────────────────────────────────────────────────────────────
from typing import Union

def filter_by_groups_and_wh(df: pd.DataFrame,
                            allow_groups: Union[list[str], None],
                            forbid_groups: Union[list[str], None],
                            allow_wh: Union[list[str], None]) -> pd.DataFrame:
    """Apply optional allowed-groups, forbidden-groups, and allowed-warehouses filters."""
    if df.empty:
        return df
    out = df.copy()
    if allow_groups is not None:
        out = out[out["itemgroup"].isin(allow_groups)]
    elif forbid_groups:
        out = out[~out["itemgroup"].isin(forbid_groups)]
    if allow_wh is not None:
        out = out[out["warehouse"].isin(allow_wh)]
    return out

def remaining_pool_topN(df: pd.DataFrame, counted_codes: set[str]) -> pd.DataFrame:
    """Exclude already-counted, keep top-N by stockvalue."""
    if df.empty:
        return df
    rem = df[~df["itemcode"].isin(counted_codes)]
    if rem.empty:
        return rem
    rem = rem.sort_values("stockvalue", ascending=False).head(POOL_N).copy()
    return rem

def choose_zids_uniform(eligible_zids: list[int]) -> list[int]:
    if not eligible_zids:
        return []
    k = min(ZIDS_PER_DAY, len(eligible_zids))
    return random.sample(eligible_zids, k=k)

def weighted_sample(df: pd.DataFrame, n: int) -> pd.DataFrame:
    if df.empty:
        return df
    weights = df["stockvalue"].clip(lower=0)
    if weights.sum() == 0:
        # If all weights are zero, sample uniformly
        return df.sample(n=min(n, len(df)))
    return df.sample(n=min(n, len(df)), weights=weights)

# ────────────────────────────────────────────────────────────────────
# EMAIL
# ────────────────────────────────────────────────────────────────────
def build_html(counter_name: str, today: str,
               fg_rows: list[dict], raw_rows: list[dict], raw_hdr: Union[str, None]) -> str:

    sections = []

    # Finished Goods section
    if fg_rows:
        df_fg = pd.DataFrame(fg_rows)
        df_fg.insert(0, "Department", df_fg["zid"].map(DEPT))
        df_fg = df_fg[["Department", "zid","itemgroup", "warehouse", "itemcode", "itemname"]]
        sections.append("<h3>Finished Goods (Daily)</h3>" +
                        df_fg.to_html(index=False, border=0, justify="left"))
    else:
        sections.append("<h3>Finished Goods (Daily)</h3><p><em>No eligible items today.</em></p>")

    # Raw/Packaging section (only when scheduled)
    if raw_hdr is not None:
        if raw_rows:
            df_raw = pd.DataFrame(raw_rows)
            df_raw.insert(0, "Department", df_raw["zid"].map(DEPT))
            df_raw = df_raw[["Department", "zid", "itemgroup", "warehouse", "itemcode", "itemname"]]
            sections.append(f"<h3>Raw / Packaging Audit (Monthly) – {raw_hdr}</h3>" +
                            df_raw.to_html(index=False, border=0, justify="left"))
        else:
            sections.append(f"<h3>Raw / Packaging Audit (Monthly) – {raw_hdr}</h3>"
                            "<p><em>No eligible items today.</em></p>")

    intro = (f"<p>Dear {counter_name},<br>"
             f"Please perform blind counts for the items listed below on {today}.</p>")
    return intro + "\n".join(sections)

def send_email(recipients: list[str], subject: str, html_body: str):
    msg = MIMEMultipart("alternative")
    msg["From"]    = SMTP["user"]
    msg["To"]      = ", ".join(recipients)
    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html"))
    with smtplib.SMTP(f"{SMTP['host']}:{SMTP['port']}") as s:
        s.starttls()
        s.login(SMTP["user"], SMTP["pwd"])
        s.sendmail(SMTP["user"], recipients, msg.as_string())

# ────────────────────────────────────────────────────────────────────
# MAIN
# ────────────────────────────────────────────────────────────────────
def main():
    today = date.today()
    weekday = today.strftime("%A")
    if weekday in SKIP_DAYS:
        return

    engine = create_engine(DB_URL)
    inv_by_zid = pull_inventory_for_all(ALL_ZIDS, engine)

    # ---------- Finished Goods (daily) ----------
    fg_rows = []
    eligible_zids = []
    fg_pools = {}

    for zid in ALL_ZIDS:
        df = inv_by_zid[zid]

        # FG categories: explicit whitelist if provided; otherwise exclude RAW categories for that zid
        allow_groups = FG_GROUPS_BY_ZID.get(zid, None)
        forbid_groups = RAW_GROUPS_BY_ZID.get(zid, None) if allow_groups is None else None

        # FG warehouses: zid-specific list, or None for "allow any"
        allow_wh = FG_WAREHOUSES_BY_ZID.get(zid, None)

        df_fg = filter_by_groups_and_wh(df, allow_groups, forbid_groups, allow_wh)
        counted_fg = load_fg_counted(zid, today)
        pool = remaining_pool_topN(df_fg, counted_fg)
        fg_pools[zid] = pool
        if not pool.empty:
            eligible_zids.append(zid)

    chosen_zids = choose_zids_uniform(eligible_zids)

    for zid in chosen_zids:
        picks = weighted_sample(fg_pools[zid], ITEMS_PER_ZID)
        if picks.empty:
            continue
        append_fg_counted(zid, picks["itemcode"].tolist(), today)
        fg_rows.extend(picks.to_dict("records"))

    # ---------- RAW / Packaging (monthly schedule) ----------
    raw_rows = []
    raw_hdr = None
    dom = today.day

    for zid, schedule in MONTHLY_RAW_SCHEDULE.items():
        if dom in schedule:
            group_name = schedule[dom]
            raw_hdr = f"{DEPT.get(zid, zid)} – {group_name}"

            df = inv_by_zid.get(zid, pd.DataFrame())
            # RAW: zid-specific warehouse filter
            allow_wh_raw = RAW_WAREHOUSES_BY_ZID.get(zid, [])
            df_raw = filter_by_groups_and_wh(df,
                                             allow_groups=[group_name],
                                             forbid_groups=None,
                                             allow_wh=allow_wh_raw)

            # rolling 90-day exclusion
            raw_log = load_raw_log(zid)  # {itemcode: "YYYY-MM-DD", ...}
            ineligible = {code for code, day in raw_log.items() if is_within_90_days(day, today)}
            pool = df_raw[~df_raw["itemcode"].isin(ineligible)]
            pool = pool.sort_values("stockvalue", ascending=False).head(POOL_N)

            picks = weighted_sample(pool, RAW_ITEMS_PER_DAY)
            if not picks.empty:
                # update rolling log with today's date
                for code in picks["itemcode"].tolist():
                    raw_log[code] = today.isoformat()
                # prune anything older than 120 days occasionally (keep file tidy)
                older_cut = today - timedelta(days=120)
                raw_log = {c: d for c, d in raw_log.items()
                           if datetime.fromisoformat(d).date() >= older_cut}
                save_raw_log(zid, raw_log)

                raw_rows.extend(picks.to_dict("records"))
            # Only one scheduled RAW group per day across configured zids
            break

    # ---------- Email ----------
    html = build_html(COUNTER_NAME, today.isoformat(), fg_rows, raw_rows, raw_hdr)
    print(html)
    subject = f"Cycle Count – {today.isoformat()}"
    send_email(COUNTER_EMAILS, subject, html)

    print(f"{today}: FG {len(fg_rows)} items across {len(chosen_zids)} zids; "
          f"RAW {len(raw_rows)} items{' ('+raw_hdr+')' if raw_hdr else ''}.")

# ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr)
        sys.exit(1)
