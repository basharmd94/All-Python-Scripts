from sqlalchemy import create_engine, text
import pandas as pd
from typing import Tuple, Dict
import matplotlib.pyplot as plt
import numpy as np
import calendar
from collections import defaultdict
from datetime import date
import os
import sys
from dotenv import load_dotenv

# === Load Environment & Config ===
load_dotenv()
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from project_config import DATABASE_URL




engine = create_engine(DATABASE_URL)

# zids
zid = [100000,100001,100005]

query = """
       SELECT glmst.zid, gldetail.xproj AS project, gldetail.xvoucher AS voucher,
       gldetail.xsub  AS cusid, cacus.xshort AS cusname, cacus.xcity AS area,
       glheader.xdate AS date, glheader.xyear AS year, glheader.xper AS month,
       SUM(gldetail.xprime) AS value
        FROM glmst
        JOIN gldetail ON glmst.xacc = gldetail.xacc AND glmst.zid = gldetail.zid
        JOIN glheader ON gldetail.xvoucher = glheader.xvoucher AND glheader.zid = glmst.zid
        JOIN cacus ON gldetail.xsub = cacus.xcus AND cacus.zid = glmst.zid
        WHERE glmst.zid = %s
          AND glmst.xaccusage = 'AR'
          AND glheader.xyear IN %s
        GROUP BY
          glmst.zid,
          gldetail.xproj,
          gldetail.xvoucher,
          gldetail.xsub,
          cacus.xshort,
          cacus.xcity,
          glheader.xdate,
          glheader.xyear,
          glheader.xper
        ORDER BY date, voucher;
"""

current_year = date.today().year      
last_year    = current_year - 1  
year_tuple = (last_year, current_year)


# fixed functions
def add_voucher_type_ar(filtered_data_ar):
    filtered_data_ar['voucher_type'] = filtered_data_ar['voucher'].str.extract(r"^([A-Za-z]+)")

    voucher_type_dict = {
    'OB':'Opening', #done
    'INOP':'Sales', #done
    'RCT':'Collection', #done
    'SRJV':'Return', #done
    'SRT':'Return', #done
    'JV':'Adjustment', #done
    'IMSA':'Sales', #done
    'STJV':'Collection', #done
    'CPAY':'Adjustment', #done
    'PAY':'Adjustment', #done
    'BRCT':'Collection', #done
    'CRCT':'Collection', #done
    'CHQ':'Collection', #done
    'ADJV':'Collection', #done
    'TR':'Adjustment', #done
    'BTJV':'Collection' #done
    }

    filtered_data_ar['voucher_type_desc'] = filtered_data_ar['voucher_type'].map(voucher_type_dict)
    filtered_data_ar["sign"] = filtered_data_ar["value"].apply(
        lambda x: 1 if x > 0 else 
                  -1 if x < 0 else 
                  0
    )
    filtered_data_ar = filtered_data_ar.sort_values(['cusid','date'])
    filtered_data_ar['value'] = pd.to_numeric(filtered_data_ar['value'], errors='coerce').fillna(0)

    now = pd.Timestamp.now()
    current_year, current_month = now.year, now.month
    filtered_data_ar = filtered_data_ar[
        (filtered_data_ar['year'] < current_year) |
        ((filtered_data_ar['year'] == current_year) &
        (filtered_data_ar['month'] < current_month))
    ]
    # Add running balance
    filtered_data_ar['running_balance'] = filtered_data_ar.groupby(['cusid','year'])['value'].cumsum()
    return filtered_data_ar

def compute_payment_timeliness_metrics(df: pd.DataFrame,tolerance: float = 10,on_time_limit: int = 30) -> Dict[str, pd.DataFrame]:
    # 1) Parse dates
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'], dayfirst=True, errors='coerce')

    #list of opening balances per customer (opening 2024) 
    ob = (df[df['voucher_type_desc']=='Opening'].groupby(['cusid','cusname','area','year'])['value'].sum().rename('opening_balance').reset_index())
    ob['opening_balance'] = pd.to_numeric(ob['opening_balance'], errors='coerce').fillna(0.0)
    ledger_txns = df[df['voucher_type_desc']!='Opening'].copy()
    ledger_txns['value'] = pd.to_numeric(ledger_txns['value'], errors='coerce').fillna(0.0)

    # 3) Invoices table
    invoices = (
        ledger_txns[ledger_txns['voucher_type_desc']=='Sales']
        .rename(columns={'date':'invoice_date','value':'invoice_amount'})
        [['cusid','cusname','area','invoice_date','invoice_amount','year','month','running_balance']]
    )
    invoices['prior_balance'] = invoices['running_balance'] - invoices['invoice_amount']

    # 4) FIFO allocation
    paid = []
    for cusid, grp in invoices.groupby('cusid'):
        cust_led = ledger_txns.loc[ledger_txns['cusid']==cusid,['date','running_balance']].reset_index(drop=True)
        for _, inv in grp.iterrows():
            thresh = inv['prior_balance'] + tolerance
            sub = cust_led[cust_led['date']>=inv['invoice_date']]
            mask = sub['running_balance'] <= thresh
            paid.append(sub.loc[mask,'date'].iloc[0] if mask.any() else pd.NaT)
    invoices['paid_date'] = pd.to_datetime(pd.Series(paid, index=invoices.index))
    invoices['days_to_pay'] = (invoices['paid_date'] - invoices['invoice_date']).dt.days
    invoices['pay_bucket'] = pd.cut(invoices['days_to_pay'], bins=[0,10,20,30,40,np.inf],labels=['0–10','11–20','21–30','31–40','>45'], right=True)

    # 5) Closing balances per cust-year-month
    opening_per_year = ob.groupby('year')['opening_balance'].sum().reset_index(name='opening_balance')
    tx = (ledger_txns.groupby(['year','month'])['value'].sum().reset_index(name='month_txn_sum'))
    tx = tx.merge(opening_per_year, on=['year'], how='left').fillna({'opening_balance':0})
    tx = tx.sort_values(['year','month'])
    tx['month_txn_sum'] = pd.to_numeric(tx['month_txn_sum'], errors='coerce').fillna(0)
    tx['cum_tx'] = tx.groupby(['year','month'])['month_txn_sum'].cumsum()
    tx['closing_balance'] = tx['opening_balance'] + tx['cum_tx']

    # 6) Year-Month Summary
    ym = tx.groupby(['year','month'])['closing_balance'].sum().rename('Total_Balance')
    sales_ym = invoices.groupby(['year','month'])['invoice_amount'].sum().rename('Total_Sales')
    col_ym = ledger_txns[ledger_txns['value']<0].groupby(['year','month'])['value'].sum().abs().rename('Total_Collection')
    ret_ym = ledger_txns[ledger_txns['voucher_type_desc']=='Return'].groupby(['year','month'])['value'].sum().abs().rename('Total_Returns')
    ym_summary = (pd.concat([ym, sales_ym, col_ym, ret_ym], axis=1).reset_index().fillna(0))

    # 7) Bucket Summary per selected year (placeholder, year param later)
    bucket_info = invoices.copy()  # raw data to produce bucket summary

    # 8) Days-to-Pay Summary per customer-year
    def safe_p90(s): arr = s.dropna().to_numpy(); return np.nan if arr.size==0 else np.percentile(arr,90)
    def pct_on_time(s): return (s <= on_time_limit).mean()
    days = (invoices.groupby(['cusid','cusname','area','year'])
            .agg(Avg_Days_to_Pay=('days_to_pay','mean'),
                 Med_Days_to_Pay=('days_to_pay','median'),
                 P90_Days_to_Pay=('days_to_pay', safe_p90),
                 Pct_On_Time=('days_to_pay', pct_on_time),
                 Total_Sales=('invoice_amount','sum'))
            .reset_index())
    days_summary = days.pivot_table(index=['cusid','cusname','area'], columns='year',
                                    values=['Avg_Days_to_Pay','Med_Days_to_Pay','P90_Days_to_Pay','Pct_On_Time'])
    days_summary.columns = [f"{m}_{y}" for m,y in days_summary.columns]; days_summary=days_summary.reset_index()

    # 9) DSO Summary per customer-month
    last_bal = (ledger_txns.groupby(['cusid','cusname','area','year','month'])['running_balance']
                .last().reset_index(name='AR_Balance'))
    cr_sales = (invoices.groupby(['cusid','cusname','area','year','month'])['invoice_amount']
                .sum().reset_index(name='Credit_Sales'))
    dso = last_bal.merge(cr_sales, on=['cusid','cusname','area','year','month'], how='left').fillna(0)
    dso[['AR_Balance','Credit_Sales']] = dso[['AR_Balance','Credit_Sales']].astype(float)
    dso['days_in_period'] = [calendar.monthrange(int(y), int(m))[1] for y, m in zip(dso['year'], dso['month'])]
    dso['DSO'] = np.where(dso['Credit_Sales']>0,
                          (dso['AR_Balance']/dso['Credit_Sales'])*dso['days_in_period'],0)
    dso_summary = dso.pivot(index=['cusid','cusname','area'], columns=['year','month'], values='DSO')
    dso_summary.columns = [f"DSO_{yr}_{mn:02d}" for yr,mn in dso_summary.columns]
    dso_summary = dso_summary.reset_index().fillna(0)

    # We will just take the total and show it below the header for how much non-sales transactions happened within the period
    cus_has_no_sales = (ledger_txns.groupby('cusid')['voucher_type_desc'].apply(lambda vs: ~vs.eq('Sales').any()))
    no_sales_cus = cus_has_no_sales[cus_has_no_sales].index.tolist()
    no_sales_rows = ledger_txns[ledger_txns['cusid'].isin(no_sales_cus)]
    no_non_sales_cust = no_sales_rows['cusid'].nunique()
    total_non_sales_balance = no_sales_rows.sort_values(['cusid', 'date']).groupby('cusid')['running_balance'].last().sum()

    led_pairs = (ledger_txns.loc[:, ['cusid','cusname','area','year']].drop_duplicates())
    check = ob.merge(led_pairs,on=['cusid','cusname','area','year'],how='left',indicator=True)
    no_tx = check[check['_merge']=='left_only'][['cusid','cusname','area','year','opening_balance']]

    return {
        'ym_summary': ym_summary,
        'bucket_info': bucket_info,
        'days_summary': days_summary,
        'dso_summary': dso_summary,
        'invoices': invoices,
        'ledger_txns': ledger_txns,
        'closing_balances': tx,
        'total_non_sales_balance': total_non_sales_balance,
        'no_non_sales_cust': no_non_sales_cust,
        'no_tx': no_tx,
        'df': df
    }

# 1. Compute base metrics
def compute_base_metrics(filtered_data_ar):
    data = compute_payment_timeliness_metrics(filtered_data_ar)
    invoices = data['invoices']
    ledger_txns = data['ledger_txns']
    dso_summary = data['dso_summary']

    df_days = (invoices.groupby(['cusid', 'cusname', 'area'])['days_to_pay'].mean().reset_index(name='Days_to_Pay'))

    dso_cols = [col for col in dso_summary.columns if col.startswith('DSO_')]
    df_dso = (dso_summary[['cusid'] + dso_cols].copy())
    df_dso['DSO'] = df_dso[dso_cols].mean(axis=1)
    df_dso = df_dso[['cusid', 'DSO']]

    df_sales = (invoices.groupby(['cusid', 'cusname', 'area'])['invoice_amount'].sum().reset_index(name='Total_Sales'))
    df_coll = (ledger_txns[ledger_txns['value'] < 0].groupby('cusid')['value'].sum().abs().reset_index(name='Total_Collection'))
    end_bal = (filtered_data_ar.sort_values(['cusid', 'date']).groupby('cusid')['running_balance'].last().reset_index(name='Ending_Balance'))
    df_final = (df_days.merge(df_dso, on='cusid', how='left').merge(df_sales, on=['cusid', 'cusname', 'area'], how='left').merge(df_coll, on='cusid', how='left').merge(end_bal, on='cusid', how='left').fillna(0))
    return df_final

def compute_composite_scores(df_final):
    # — 1) Compute min/max for each metric —
    mins = df_final[['Days_to_Pay','Total_Collection','Total_Sales','DSO']].min()
    maxs = df_final[['Days_to_Pay','Total_Collection','Total_Sales','DSO']].max()
    denom = (maxs - mins).replace(0, 1)  # avoid division by zero

    # — 2) Normalize each metric to [0,1] —
    #   • For Days_to_Pay & DSO: lower is better, so invert
    df_final['norm_days']  = (maxs['Days_to_Pay']     - df_final['Days_to_Pay'])     / denom['Days_to_Pay']
    df_final['norm_dso']   = (maxs['DSO']              - df_final['DSO'])             / denom['DSO']
    #   • For Sales & Collection: higher is better
    df_final['norm_sales'] = (df_final['Total_Sales']  - mins['Total_Sales'])           / denom['Total_Sales']
    df_final['norm_coll']  = (df_final['Total_Collection'] - mins['Total_Collection']) / denom['Total_Collection']

    # — 3) Define your weights (and normalize them to sum=1) —
    w_days, w_coll, w_sales, w_dso = 1.0, 0.9, 0.7, 0.1
    total_w = w_days + w_coll + w_sales + w_dso
    w_days  /= total_w
    w_coll  /= total_w
    w_sales /= total_w
    w_dso   /= total_w

    # — 4) Compute Composite Score —
    df_final['Composite_Score'] = (
        w_days  * df_final['norm_days'] +
        w_dso   * df_final['norm_dso'] +
        w_sales * df_final['norm_sales'] +
        w_coll  * df_final['norm_coll']
    )

    # — 5) (Optional) drop the intermediate norms —
    df_final.drop(columns=['norm_days','norm_dso','norm_sales','norm_coll'], inplace=True)

    return df_final

def make_dynamic_bins(scores: pd.Series,seed_width: float = 0.1,top_n: int = 3,split_pcts: list = [0.25, 0.5, 0.75, 0.99]) -> list:
    seed_edges = list(np.arange(0, 1 + seed_width, seed_width))
    seed_bins = pd.cut(
        scores,
        bins=seed_edges,
        right=False,
        include_lowest=True
    )
    top_seeds = seed_bins.value_counts().nlargest(top_n).index
    all_edges = set(seed_edges)
    for interval in top_seeds:
        low, high = interval.left, interval.right
        subset = scores[(scores >= low) & (scores < high)]
        if len(subset) >= len(split_pcts):
            qvals = subset.quantile(split_pcts).tolist()
            for q in qvals:
                if low < q < high:
                    all_edges.add(q)
    return sorted(all_edges)

def generate_summary_table(df_final):
    edges = make_dynamic_bins(df_final['Composite_Score'])
    labels = [f"{edges[i]:.3f}-{edges[i+1]:.3f}" for i in range(len(edges)-1)]
    df_final['comp_bin'] = pd.cut(
        df_final['Composite_Score'],
        bins=edges,
        labels=labels,
        right=False,
        include_lowest=True,
        ordered=False
    )

    # 4) Summary table per dynamic bin
    summary_table = (
        df_final
        .groupby('comp_bin')
        .agg(
            Count=('cusid','nunique'),
            Avg_Days_To_Pay=('Days_to_Pay','mean'),
            Total_Sales=('Total_Sales','sum'),
            Total_Collection=('Total_Collection','sum'),
            Total_Ending_Balance=('Ending_Balance','sum')
        )
        .reset_index()
    )

    segment_map = {
        "0.0-0.1": "Critical Watch",
        "0.1-0.2": "High Risk",
        "0.2-0.3": "Warning Zone",
        "0.3-0.4": "Needs Attention",
        "0.4-0.5": "Developing",
        "0.5-0.6": "Stable",
        "0.6-0.7": "Solid Performer",
        "0.7-0.8": "Valued Partner",
        "0.8-0.9": "Top Tier",
        "0.9-1.0": "Elite Champion"
    }
    
    orig_intervals = [
        (float(k.split('-')[0]), float(k.split('-')[1]), v)
        for k, v in segment_map.items()
    ]

    # map each dynamic label to its base
    bin_to_base = {}
    for lbl in labels:
        low = float(lbl.split('-')[0])
        for o_low, o_high, base in orig_intervals:
            if (o_low <= low < o_high) or (low == 1.0 and o_high == 1.0):
                bin_to_base[lbl] = base
                break

    # group and sort
    grouped = defaultdict(list)
    for lbl, base in bin_to_base.items():
        grouped[base].append(lbl)

    lbl_to_segment = {}
    for base, lbls in grouped.items():
        sorted_lbls = sorted(lbls, key=lambda x: float(x.split('-')[0]))
        for idx, lbl in enumerate(sorted_lbls):
            lbl_to_segment[lbl] = f"{base}-{idx}"

    df_final['segment_name'] = df_final['comp_bin'].astype(str).map(lbl_to_segment)


    return summary_table, df_final

def update_sql(df_final, zid, engine):

    with engine.begin() as conn:
        update_stmt = text("""
            UPDATE cacus
            SET xtitle   = :segment_name,
                xfax     = :comp_bin
            WHERE xcus     = :xcus
            AND zid      = :zid
        """)
        for _, row in df_final.iterrows():
            conn.execute(update_stmt, {
                "segment_name": row['segment_name'],
                "comp_bin":     row['comp_bin'],
                "xcus":         row['cusid'],
                "zid":          zid
            })
        engine.dispose()


# db connection


# GI update
params = (zid[0], year_tuple)
df_gi = pd.read_sql(query, engine, params=params)
df_gi = add_voucher_type_ar(df_gi)
df_gi = compute_base_metrics(df_gi)
df_gi = compute_composite_scores(df_gi)
df_gi_summary, df_gi = generate_summary_table(df_gi)
update_sql(df_gi, zid[0], engine)
print(df_gi.count(),'completed gi')
# Trade update
params = (zid[1], year_tuple)
df_trade = pd.read_sql(query, engine, params=params)
df_trade = add_voucher_type_ar(df_trade)
df_trade = compute_base_metrics(df_trade)
df_trade = compute_composite_scores(df_trade)
df_trade_summary, df_trade = generate_summary_table(df_trade)
update_sql(df_trade, zid[1], engine)
print(df_trade.count(),'completed trade')
# zepto update
params = (zid[2], year_tuple)
df_zepto = pd.read_sql(query, engine, params=params)
df_zepto = add_voucher_type_ar(df_zepto)
df_zepto = compute_base_metrics(df_zepto)
df_zepto = compute_composite_scores(df_zepto)
df_zepto_summary, df_zepto = generate_summary_table(df_zepto)
update_sql(df_zepto, zid[2], engine)
print(df_zepto.count(),'completed zepto')

df_gi_summary.to_csv('df_gi_summary.csv', index=False)
df_trade_summary.to_csv('df_trade_summary.csv', index=False)
df_zepto_summary.to_csv('df_zepto_summary.csv', index=False)