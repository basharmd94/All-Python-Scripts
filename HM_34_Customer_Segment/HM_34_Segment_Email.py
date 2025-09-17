from sqlalchemy import create_engine, text
import pandas as pd
from datetime import date
import re
import os
import sys
from dotenv import load_dotenv

# === Load Environment & Config ===
load_dotenv()
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


from mail import send_mail, get_email_recipients
from project_config import DATABASE_URL

engine = create_engine(DATABASE_URL)

zid = [100000,100001,100005]

query = """SELECT 
                opord.xordernum, 
                opord.xcus, 
                cacus.xorg, 
                opord.xdiv, 
                opord.xdtwotax, 
                opord.xstatusord, 
                opord.xdate, 
                cacus.xtitle, 
                cacus.xfax
        FROM opord 
        JOIN cacus
            ON opord.xcus = cacus.xcus 
            AND opord.zid = cacus.zid
        WHERE opord.zid = %s
        AND opord.xdate = %s
        """

today_date = date.today().strftime('%Y-%m-%d') 

# today_date = '2025-05-28'
df_gi = pd.read_sql(query, engine, params=(zid[0],today_date))
df_trade = pd.read_sql(query, engine, params=(zid[1],today_date))
df_zep = pd.read_sql(query, engine, params=(zid[2],today_date))

# ── ranking helpers (xtitle = segment, xfax = comp_bin) ────────────────────────
segment_tiers = [
    "Critical Watch", "High Risk", "Warning Zone",
    "Needs Attention", "Developing", "Stable",
    "Solid Performer", "Valued Partner", "Top Tier", "Elite Champion"
]
tier_rank = {tier: i for i, tier in enumerate(segment_tiers)}   # 0 = worst
seg_pat   = re.compile(r"^(.*?)\s*(?:-(\d))?$")                  # e.g. Needs Attention-3

def _split(label: str):
    """'Warning Zone-2' → ('Warning Zone', 2);  'Critical Watch' → ('Critical Watch', 0)"""
    if pd.isna(label):
        return ("Unknown", 0)
    m = seg_pat.match(str(label).strip())
    base, suf = m.groups() if m else (label, None)
    return base, int(suf) if suf is not None else 0

def _filter_sort(df):
    # explode xtitle into pieces
    df = df.copy()
    df[["seg_base", "seg_suf"]] = df["xtitle"].apply(_split).apply(pd.Series)
    df["seg_rank"] = df["seg_base"].map(tier_rank).fillna(len(segment_tiers))

    # keep everything worse than Needs Attention-3-4
    worst_na = tier_rank["Needs Attention"]
    mask = (df["seg_rank"] < worst_na) | ((df["seg_rank"] == worst_na) & (df["seg_suf"] <= 2))
    df = df[mask]

    # sort: tier (0→…), suffix (0→4), customer, order
    df.sort_values(by=["seg_rank", "seg_suf", "xcus", "xordernum"], inplace=True)

    # drop helpers
    return df.drop(columns=["seg_base", "seg_suf", "seg_rank"])

df_gi = _filter_sort(df_gi)
df_trade = _filter_sort(df_trade)
df_zep = _filter_sort(df_zep)

try:
    # Extract report name from filename
    report_name = os.path.splitext(os.path.basename(__file__))[0]
    print(report_name)
    recipients = get_email_recipients(report_name)
    print(f"📬 Recipients: {recipients}")
except Exception as e:
    print(f"⚠️ Failed to fetch recipients: {e}")
    recipients = ["ithmbrbd@gmail.com"]  # Fallback


send_mail(
    "H_34 Customer Segmentation", "Please find the attachment.\n",
     html_body=[(df_gi, 'GI Corporation'), (df_trade, 'GULSHAN TRADING'), (df_zep, 'ZEPTO Chemicals')], 
     recipient = recipients
     )

