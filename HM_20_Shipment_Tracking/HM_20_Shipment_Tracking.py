"""
📦 HM_20_Shipment_Tracking.py – Shipment Profitability & Tracking Report

🚀 PURPOSE:
    - Track inbound shipments (GRN) from HMBR & Packaging
    - Link packaging codes to finished goods via pack_dict
    - For each GRN:
        - Get pre-stock (before GRN date)
        - Get sales (after GRN to today)
        - Calculate GP%, progress vs potential
    - Flag items with <100% GP achievement
    - Export to multi-sheet Excel + send summary email

🔧 PRESERVED:
    - All SQL queries are 100% unchanged (copy-paste from original)
    - All variable names, logic, loops, and flow preserved
    - Same pack_dict mapping
    - Same np.where() logic for grunnum/date
    - Same final_dict merge logic

📬 EMAIL:
    - Recipients: get_email_recipients("HM_20_Shipment_Tracking")
    - Fallback: ithmbrbd@gmail.com
    - Subject: "shipment track"

📁 OUTPUT:
    - shipment1.xlsx → one sheet per GRN
    - shipment2.xlsx → underperforming items
    - shipment.html → summary table
    - Email with HTML + both attachments
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from dotenv import load_dotenv


# ─────────────────────────────────────────────────────────────────────
# 🌍 1. Load Environment & Setup
# ─────────────────────────────────────────────────────────────────────
load_dotenv()

print("🌍 Loading configuration...")

# Load ZIDs from .env
try:
    ZID_HMBR = int(os.environ["ZID_GULSHAN_TRADING"])      # 100001
    ZID_PACKAGING = int(os.environ["ZID_GULSHAN_PACKAGING"])  # 100009
except KeyError as e:
    raise RuntimeError(f"❌ Missing ZID in .env: {e}")

# Date Setup
end_date = datetime.now().strftime("%Y-%m-%d")
start_date = (datetime.now() - timedelta(days=1400)).strftime("%Y-%m-%d")

print(f"📅 Data Window: {start_date} → {end_date}")


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
# 📥 4. Fetch Data Functions (Same Query Strings)
# ─────────────────────────────────────────────────────────────────────
def get_igrn(zid, start_date):
    query = f"""SELECT pogrn.xgrnnum, pogrn.xdate, poodt.xitem
                FROM poord 
                JOIN poodt
                ON poord.xpornum = poodt.xpornum
                JOIN pogrn
                ON poord.xpornum = pogrn.xpornum
                WHERE poord.zid= '{zid}'
                AND poodt.zid = '{zid}'
                AND pogrn.zid = '{zid}'
                AND poord.xpornum LIKE 'IP--%%'
                AND poord.xstatuspor = '5-Received'
                AND poord.xdate > '{start_date}'
                GROUP BY pogrn.xgrnnum, pogrn.xdate, poodt.xitem"""
    return pd.read_sql_query(query, con=engine)

def get_caitem(zid):
    query = f"""SELECT xitem, xdesc, xgitem, xcitem, xpricecat, xduty, xwh, xstdcost, xstdprice
                FROM caitem 
                WHERE zid = '{zid}'
                AND xgitem = 'Hardware'
                OR xgitem = 'Furniture Fittings'
                OR xgitem = 'Industrial & Household'
                OR xgitem = 'Sanitary'
                ORDER BY xgitem ASC"""
    return pd.read_sql_query(query, con=engine)

def get_stock(zid, end_date):
    query = f"""SELECT imtrn.xitem, SUM(imtrn.xqty*imtrn.xsign) as Stock
                FROM imtrn
                WHERE imtrn.zid = '{zid}'
                AND imtrn.xdate <= '{end_date}'
                GROUP BY imtrn.xitem"""
    return pd.read_sql_query(query, con=engine)

def get_item_stock(zid, end_date, item):
    # Format the IN clause manually
    if isinstance(item, tuple):
        item_str = str(item)
    else:
        item_str = f"('{item}')"
    query = f"""SELECT imtrn.xitem, SUM(imtrn.xqty*imtrn.xsign) as Stock
                FROM imtrn
                WHERE imtrn.zid = '{zid}'
                AND imtrn.xdate < '{end_date}'
                AND imtrn.xitem IN {item_str}
                GROUP BY imtrn.xitem"""
    return pd.read_sql_query(query, con=engine)

def get_item_stock_1(zid, end_date, item):
    query = f"""SELECT imtrn.xitem, SUM(imtrn.xqty*imtrn.xsign) as Stock
                FROM imtrn
                WHERE imtrn.zid = '{zid}'
                AND imtrn.xdate < '{end_date}'
                AND imtrn.xitem = '{item}'
                GROUP BY imtrn.xitem"""
    return pd.read_sql_query(query, con=engine)

def get_special_price(zid):
    query = f"""SELECT xpricecat, xqty,xdisc
                FROM opspprc 
                WHERE zid = '{zid}'"""
    return pd.read_sql_query(query, con=engine)

def get_sales(zid, start_date, end_date, item):
    if isinstance(item, tuple):
        item_str = str(item)
    else:
        item_str = f"('{item}')"
    query = f"""SELECT imtrn.xitem, sum(imtrn.xqty*imtrn.xsign) as sales
                FROM imtrn
                WHERE imtrn.zid = '{zid}'
                AND imtrn.xdocnum LIKE 'DO--%%'
                AND imtrn.xdate >= '{start_date}'
                AND imtrn.xdate <= '{end_date}'
                AND imtrn.xitem IN {item_str}
                GROUP BY imtrn.xitem"""
    return pd.read_sql_query(query, con=engine)

def get_sales_1(zid, start_date, end_date, item):
    query = f"""SELECT imtrn.xitem, sum(imtrn.xqty*imtrn.xsign) as sales
                FROM imtrn
                WHERE imtrn.zid = '{zid}'
                AND imtrn.xdocnum LIKE 'DO--%%'
                AND imtrn.xdate >= '{start_date}'
                AND imtrn.xdate <= '{end_date}'
                AND imtrn.xitem = '{item}'
                GROUP BY imtrn.xitem"""
    return pd.read_sql_query(query, con=engine)

def get_purchase(zid, start_date):
    query = f"""SELECT poodt.xitem, poord.xcounterno, poodt.xqtyord, poodt.xrate, pogrn.xgrnnum, pogrn.xdate
                FROM poord 
                JOIN poodt
                ON poord.xpornum = poodt.xpornum
                JOIN pogrn
                ON poord.xpornum = pogrn.xpornum
                WHERE poord.zid= '{zid}'
                AND poodt.zid = '{zid}'
                AND pogrn.zid = '{zid}'
                AND poord.xpornum LIKE 'IP--%%'
                AND poord.xstatuspor = '5-Received'
                AND poord.xdate > '{start_date}'"""
    return pd.read_sql_query(query, con=engine)


# ─────────────────────────────────────────────────────────────────────
# 📦 5. Packaging Mapping (Silent)
# ─────────────────────────────────────────────────────────────────────
pack_dict = {
    'HPI000001': '0119', 'HPI000002': '0120', 'HPI000003': '0121', 'HPI000004': '1640',
    'HPI000005': '2154', 'HPI000006': '0186', 'HPI000007': '2155', 'HPI000009': '0458',
    'HPI000010': '0459', 'HPI000011': '0706', 'HPI000012': '0717', 'HPI000013': '0718',
    'HPI000014': '0719', 'HPI000015': '0720', 'HPI000016': '0721', 'HPI000017': '0722',
    'HPI000018': '0723', 'HPI000019': '0724', 'HPI000020': '0725', 'HPI000021': '0726',
    'HPI000022': '0727', 'HPI000026': '1122', 'HPI000027': '1126', 'HPI000028': '1128',
    'HPI000029': '1129', 'HPI000030': '1130', 'HPI000031': '1131', 'HPI000032': '1139',
    'HPI000033': '1140', 'HPI000034': '1141', 'HPI000035': '1142', 'HPI000036': '1143',
    'HPI000037': '1150', 'HPI000038': '1153', 'HPI000039': '1154', 'HPI000045': '1198',
    'HPI000046': '12040', 'HPI000047': '1219', 'HPI000048': '1236', 'HPI000049': '12381',
    'HPI000050': '12382', 'HPI000051': '1299', 'HPI000052': '1300', 'HPI000055': '1332',
    'HPI000056': '1349', 'HPI000057': '1410', 'HPI000058': '1411', 'HPI000059': '1412',
    'HPI000060': '14351', 'HPI000061': '14352', 'HPI000062': '14361', 'HPI000063': '14362',
    'HPI000065': '1527', 'HPI000067': '1528', 'HPI000068': '1576', 'HPI000069': '1594',
    'HPI000072': '1596', 'HPI000073': '2146', 'HPI000074': '1600', 'HPI000075': '1601',
    'HPI000078': '1650', 'HPI000079': '1652', 'HPI000080': '16990', 'HPI000081': '17010',
    'HPI000082': '1767', 'HPI000087': '2046', 'HPI000088': '2047', 'HPI000089': '2048',
    'HPI000090': '2049', 'HPI000091': '2050', 'HPI000092': '2060', 'HPI000093': '2070',
    'HPI000094': '2105', 'HPI000095': '11501', 'HPI000096': '11230', 'HPI000097': '0178',
    'HPI000098': '0180', 'HPI000099': '0179', 'HPI000100': '01877', 'HPI000101': '01878',
    'HPI000102': '01879', 'HPI000103': '2111', 'HPI000104': '1577', 'HPI000105': '1578',
    'HPI000106': '1579', 'HPI000107': '2127', 'HPI000108': '2128', 'HPI000109': '2129',
    'HPI000110': '1807', 'HPI000111': '1766', 'HPI000112': '2125', 'HPI000113': '2126',
    'HPI000114': '2148', 'HPI000115': '2145'
}

df_pack = pd.DataFrame(pack_dict.items(), columns=['pack_code', 'xitem'])
df_pack = df_pack[['xitem', 'pack_code']]
# No print → silent


# ─────────────────────────────────────────────────────────────────────
# 🧱 6. Build Base DataFrame
# ─────────────────────────────────────────────────────────────────────
print("🧱 Building base product data...")

df_caitem = get_caitem(ZID_HMBR)
df_sp_price = get_special_price(ZID_HMBR).rename(columns={'xpricecat': 'xitem'})
df_stock_hmbr = get_stock(ZID_HMBR, end_date)
df_stock_pack = get_stock(ZID_PACKAGING, end_date).rename(columns={'xitem': 'pack_code'})

# Merge all
dff = df_caitem.merge(df_sp_price[['xitem', 'xdisc']], on='xitem', how='left').fillna(0).rename(columns={'xstdprice': 'retailP'})
dff['wholesaleP'] = dff['retailP'] - dff['xdisc']
del dff['xdisc']

dff = dff.merge(df_stock_hmbr[['xitem', 'stock']], on='xitem', how='left').fillna(0).rename(columns={'stock': 'hmbr_stock'})
dff = dff.merge(df_pack[['xitem', 'pack_code']], on='xitem', how='left').fillna(0)
dff = dff.merge(df_stock_pack[['pack_code', 'stock']], on='pack_code', how='left').fillna(0).rename(columns={'stock': 'pack_stock'})
dff['Current_Stock'] = dff['hmbr_stock'] + dff['pack_stock']


# ─────────────────────────────────────────────────────────────────────
# 📦 7. Get Purchase & GRN Data
# ─────────────────────────────────────────────────────────────────────
print("📦 Fetching purchase and GRN data...")

df_po_trade = get_purchase(ZID_HMBR, start_date)
dff = dff.merge(df_po_trade[['xitem', 'xqtyord', 'xrate', 'xgrnnum', 'xdate']], on='xitem', how='left')
df_po_pack = get_purchase(ZID_PACKAGING, start_date).rename(columns={'xitem': 'pack_code'})
dff = dff.merge(df_po_pack[['pack_code', 'xqtyord', 'xrate', 'xgrnnum', 'xdate']], on='pack_code', how='left')

# Combine with same logic
for col in ['xqtyord', 'xrate']:
    dff[f'{col}_x'] = dff[f'{col}_x'].fillna(0)
    dff[f'{col}_y'] = dff[f'{col}_y'].fillna(0)
    dff[f'Qty_order' if col == 'xqtyord' else 'p_rate'] = dff[f'{col}_x'] + dff[f'{col}_y']
    dff = dff.drop(columns=[f'{col}_x', f'{col}_y'])

for col in ['xgrnnum', 'xdate']:
    dff[f'{col}_x'] = dff[f'{col}_x'].fillna('')
    dff[f'{col}_y'] = dff[f'{col}_y'].fillna('')
    condition = dff[f'{col}_y'] != ''
    dff[col] = np.where(condition, dff[f'{col}_y'], dff[f'{col}_x'])
    dff = dff.drop(columns=[f'{col}_x', f'{col}_y'])

dff = dff.rename(columns={'xgrnnum': 'grunnum', 'xdate': 'date'})


# ─────────────────────────────────────────────────────────────────────
# 📊 8. Process by GRN (Same Loop Logic)
# ─────────────────────────────────────────────────────────────────────
print("📊 Processing by GRN...")

df_grn_trade = get_igrn(ZID_HMBR, start_date)
df_grn_pack = get_igrn(ZID_PACKAGING, start_date)

# Build date & item maps (same as original)
date_dict = {}
date_dict[ZID_HMBR] = df_grn_trade.groupby('xgrnnum')['xdate'].apply(lambda x: x.iloc[0].strftime("%Y-%m-%d")).to_dict()
date_dict[ZID_PACKAGING] = df_grn_pack.groupby('xgrnnum')['xdate'].apply(lambda x: x.iloc[0].strftime("%Y-%m-%d")).to_dict()

item_dict = {}
item_dict[ZID_HMBR] = df_grn_trade.groupby('xgrnnum')['xitem'].apply(list).to_dict()
item_dict[ZID_PACKAGING] = df_grn_pack.groupby('xgrnnum')['xitem'].apply(list).to_dict()

main_dict_trade = {}
for (dk, dv), (ik, iv) in zip(date_dict[ZID_HMBR].items(), item_dict[ZID_HMBR].items()):
    df_stock = get_item_stock(ZID_HMBR, dv, tuple(iv))
    df_sales = get_sales(ZID_HMBR, dv, end_date, tuple(iv))
    df_main = dff[dff['grunnum'] == dk].copy()
    df_main = df_main.merge(df_stock, on='xitem', how='left').merge(df_sales, on='xitem', how='left').fillna(0)
    df_main = df_main.rename(columns={'stock': 'pre_stock', 'xitem': 'code', 'xdesc': 'name', 'xgitem': 'group', 'xstdcost': 'avg_cost'})
    df_main = df_main.drop(columns=['xcitem', 'xpricecat', 'xduty', 'xwh', 'hmbr_stock', 'pack_stock'])
    df_main = df_main[['code', 'pack_code', 'name', 'group', 'grunnum', 'date', 'avg_cost', 'p_rate', 'retailP', 'wholesaleP', 'pre_stock', 'Qty_order', 'sales', 'Current_Stock']]
    df_main['sales'] = np.where((df_main['sales'] * -1) > df_main['Qty_order'], df_main['Qty_order'] * -1, df_main['sales'])
    df_main['total_p_rev'] = df_main['Qty_order'] * df_main['wholesaleP'] * -1
    df_main['total_p_cost'] = df_main['Qty_order'] * df_main['p_rate']
    df_main['total_rev'] = df_main['sales'] * df_main['wholesaleP'] * -1
    df_main['total_cost'] = df_main['sales'] * df_main['p_rate']
    df_main['total_gp'] = df_main['total_p_rev'] + df_main['total_p_cost']
    df_main['gp'] = df_main['total_rev'] + df_main['total_cost']
    df_main['perc'] = (df_main['gp'] / df_main['total_gp']) * 100
    df_main = df_main.round(2)
    main_dict_trade[dk] = df_main

main_dict_pack = {}
for (dk, dv), (ik, iv) in zip(date_dict[ZID_PACKAGING].items(), item_dict[ZID_PACKAGING].items()):
    if len(iv) == 1:
        df_stock_pack = get_item_stock_1(ZID_PACKAGING, dv, iv[0])
    else:
        df_stock_pack = get_item_stock(ZID_PACKAGING, dv, tuple(iv))
    df_main = dff[dff['grunnum'] == dk].copy()
    item_list = tuple(df_main['xitem'].tolist())
    if len(item_list) == 1:
        df_stock_trade = get_item_stock_1(ZID_HMBR, dv, item_list[0])
        df_sales = get_sales_1(ZID_HMBR, dv, end_date, item_list[0])
    else:
        df_stock_trade = get_item_stock(ZID_HMBR, dv, item_list)
        df_sales = get_sales(ZID_HMBR, dv, end_date, item_list)
    df_stock_pack = df_stock_pack.rename(columns={'xitem': 'pack_code'})
    df_main = df_main.merge(df_stock_pack[['pack_code', 'stock']], on='pack_code', how='left') \
                    .merge(df_stock_trade[['xitem', 'stock']], on='xitem', how='left') \
                    .merge(df_sales[['xitem', 'sales']], on='xitem', how='left').fillna(0)
    df_main['stock'] = df_main['stock_x'] + df_main['stock_y']
    df_main = df_main.rename(columns={'stock': 'pre_stock', 'xitem': 'code', 'xdesc': 'name', 'xgitem': 'group', 'xstdcost': 'avg_cost'})
    df_main = df_main.drop(columns=['xcitem', 'xpricecat', 'xduty', 'xwh', 'hmbr_stock', 'pack_stock'])
    df_main = df_main[['code', 'pack_code', 'name', 'group', 'grunnum', 'date', 'avg_cost', 'p_rate', 'retailP', 'wholesaleP', 'pre_stock', 'Qty_order', 'sales', 'Current_Stock']]
    df_main['sales'] = np.where((df_main['sales'] * -1) > df_main['Qty_order'], df_main['Qty_order'] * -1, df_main['sales'])
    df_main['total_p_rev'] = df_main['Qty_order'] * df_main['wholesaleP'] * -1
    df_main['total_p_cost'] = df_main['Qty_order'] * df_main['p_rate']
    df_main['total_rev'] = df_main['sales'] * df_main['wholesaleP'] * -1
    df_main['total_cost'] = df_main['sales'] * df_main['p_rate']
    df_main['total_gp'] = df_main['total_p_rev'] + df_main['total_p_cost']
    df_main['gp'] = df_main['total_rev'] + df_main['total_cost']
    df_main['perc'] = (df_main['gp'] / df_main['total_gp']) * 100
    df_main = df_main.round(2)
    main_dict_pack[dk] = df_main


# ─────────────────────────────────────────────────────────────────────
# 🔗 9. Merge Final Dict (Same Logic)
# ─────────────────────────────────────────────────────────────────────
# Reverse date_dict
date_dict[ZID_HMBR] = {v: k for k, v in date_dict[ZID_HMBR].items()}
date_dict[ZID_PACKAGING] = {v: k for k, v in date_dict[ZID_PACKAGING].items()}

fdate_dict = {**date_dict[ZID_HMBR], **date_dict[ZID_PACKAGING]}
for k, v in fdate_dict.items():
    if k in date_dict[ZID_HMBR] and k in date_dict[ZID_PACKAGING]:
        fdate_dict[k] = [v, date_dict[ZID_HMBR][k]]

final_dict = {}
for k, v in fdate_dict.items():
    if isinstance(v, list):
        if v[0] in main_dict_pack:
            final_dict[k] = pd.concat([main_dict_trade[v[1]], main_dict_pack[v[0]]])
        else:
            final_dict[k] = pd.concat([main_dict_trade[v[0]], main_dict_pack[v[1]]])
    else:
        if v in main_dict_trade:
            final_dict[k] = main_dict_trade[v]


# ─────────────────────────────────────────────────────────────────────
# 📈 10. Compile Summary & Underperforming Items
# ─────────────────────────────────────────────────────────────────────
print("📈 Compiling summary...")

total_df = pd.DataFrame(columns=['code', 'pack_code', 'name', 'group', 'grunnum', 'date', 'avg_cost',
                                 'p_rate', 'retailP', 'wholesaleP', 'pre_stock', 'Qty_order', 'sales',
                                 'Current_Stock', 'total_p_rev', 'total_p_cost', 'total_rev',
                                 'total_cost', 'total_gp', 'gp', 'perc'])

item_df = total_df.copy()

for key,value in final_dict.items():
    # print (key, value)
    if value['perc'].count() != 0:
        value.loc[max(value.index)+1] = value.sum(numeric_only=True,axis=0)
        #print(max(value.index))
        value.loc[max(value.index),'code'] = 'Total'
        value.loc[max(value.index),'date'] = key
        value.loc[max(value.index),'avg_cost'] = 0
        value.loc[max(value.index),'p_rate'] = 0
        value.loc[max(value.index),'retailP'] = 0
        value.loc[max(value.index),'wholesaleP'] = 0
        value.loc[max(value.index),'pre_stock'] = 0
        value.loc[max(value.index),'Qty_order'] = 0
        value.loc[max(value.index),'sales'] = 0
        value.loc[max(value.index),'Current_Stock'] = 0
        value.loc[max(value.index),'perc'] = (value.loc[max(value.index),'gp']/value.loc[max(value.index),'total_gp'])*100
        filt = value[(value['perc']<100) & (value['code'] != 'Total')]
    total_df = total_df.append(value.loc[max(value.index)])
    item_df=item_df.append(filt)
# print('item_df_created')

item_df['date'] = pd.to_datetime(item_df['date'])
item_df['date'] = item_df['date'].apply(lambda x: x.date())
item_df = item_df.reset_index()
item_df['today'] = datetime.now().date()
item_df['diff'] = item_df['date'] - item_df['today']
item_df['diff'] = item_df['diff'].dt.days
item_df['perc/day'] = (item_df['perc']/item_df['diff'])*-1
item_df = item_df.drop(columns=['index','today'])
item_df = item_df.sort_values('perc/day')

# ─────────────────────────────────────────────────────────────────────
# 📁 11. Export to Excel
# ─────────────────────────────────────────────────────────────────────
print("📁 Exporting to Excel...")
with pd.ExcelWriter('shipment1.xlsx', engine='openpyxl') as writer:
    for k, v in final_dict.items():
        sheet_name = k.replace('-', '')[:31]
        v.to_excel(writer, sheet_name=sheet_name, index=False)

item_df.to_excel('shipment2.xlsx', sheet_name='item', index=False)


# ─────────────────────────────────────────────────────────────────────
# 📬 12. Send Email
# ─────────────────────────────────────────────────────────────────────
try:
    recipients = get_email_recipients("HM_20_Shipment_Tracking")
    print(f"📬 Recipients: {recipients}")
except Exception as e:
    print(f"⚠️ Fallback: {e}")
    recipients = ["ithmbrbd@gmail.com"]

subject = "shipment track"
body_text = """
Dear Sir,

Please find the shipment tracking report.

Includes:
- Shipment-wise performance
- Underperforming items
- GP% vs potential

See attachments and HTML summary.

Best regards,
Automated Reporting System
"""

# Save HTML
total_summary = total_df[['date', 'total_p_rev', 'total_p_cost', 'total_gp', 'gp', 'perc']].round(2)
with open('shipment.html', 'w') as f:
    f.write(f"<h3>Shipment Summary (n={len(total_df)})</h3>")
    f.write(total_summary.to_html(classes='total_df'))

send_mail(
    subject=subject,
    bodyText=body_text,
    attachment=['shipment1.xlsx', 'shipment2.xlsx'],
    recipient=recipients,
    html_body=[(total_summary, "Shipment Summary")]
)

print("✅ HM_20 completed successfully.")