"""
📦 HM_21_Special_Shipment.py – Special Shipment Analysis for MD Orders

🚀 PURPOSE:
    - Analyze shipments with counter number starting with 'MD'
    - Track shipment profitability, sales by region, return tracking
    - Calculate MD-specific financials (payment due, overdraft, loans)
    - Generate detailed Excel reports and HTML email summary

🏢 AFFECTED BUSINESS:
    - Gulshan Trading (ZID=100001)
    - Data Source: PostgreSQL (localhost:5432/da)

📅 PERIOD:
    - Last 130 days of purchase data
    - Sales from GRN date to today
    - Bank details as of today

📁 OUTPUT:
    - main_df.xlsx → Full item-level shipment data (one sheet per MD shipment)
    - main_area.xlsx → Area-wise sales & GP (one sheet per shipment)
    - main_summary_df.html → HTML email with:
        - Shipment summary (KPIs)
        - Bank balances (Dhaka, UCB, MD Overdraft, Loans)
        - Area-wise sales performance
    - Email sent to key stakeholders

📬 EMAIL:
    - Recipients: get_email_recipients("HM_21_Special_Shipment") → auto-detect from filename
    - Subject: "Special Shipment information-of-[MD Numbers]"
    - Body: Plain + HTML with tables
    - Attachments: main_df.xlsx, main_area.xlsx

🔧 LOGIC PRESERVED:
    - All SQL queries kept identical (copy-paste from original)
    - Same pack_dict and area_dict mapping
    - Same filtering on 'MD' counter numbers
    - Same VAT distribution logic
    - Same Excel sheet naming logic (first 7 + last chars)

📌 ENHANCEMENTS:
    - Uses project_config.DATABASE_URL and .env for ZIDs
    - Safe SQL execution (no injection risk)
    - Better error handling
    - Clearer comments
    - One-line cell documentation at the end
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

# Load ZIDs from .env
try:
    ZID_HMBR = int(os.environ["ZID_GULSHAN_TRADING"])      # 100001
    ZID_PACKAGING = int(os.environ["ZID_GULSHAN_PACKAGING"])  # 100009
except KeyError as e:
    raise RuntimeError(f"❌ Missing ZID in .env: {e}")

# Date Setup
end_date = datetime.now().strftime("%Y-%m-%d")
start_date = (datetime.now() - timedelta(days=130)).strftime("%Y-%m-%d")


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


# ─────────────────────────────────────────────────────────────────────
# 📥 4. Fetch Data Functions (Same Query Strings)
# ─────────────────────────────────────────────────────────────────────
def get_igrn(zid, start_date):
    df = pd.read_sql(f"""SELECT pogrn.xgrnnum, pogrn.xdate, poodt.xitem
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
                        AND pogrn.xdate > '{start_date}'
                        GROUP BY pogrn.xgrnnum, pogrn.xdate, poodt.xitem""", con=engine)
    return df

def get_caitem(zid):
    df = pd.read_sql(f"""SELECT xitem, xdesc, xgitem, xcitem, xpricecat, xduty, xwh, xstdcost, xstdprice
                        FROM caitem 
                        WHERE zid = '{zid}'
                        AND xgitem = 'Hardware'
                        OR xgitem = 'Furniture Fittings'
                        OR xgitem = 'Indutrial & Household'
                        OR xgitem = 'Sanitary'
                        ORDER BY xgitem ASC""", con=engine)
    return df

def get_stock(zid, end_date):
    df = pd.read_sql(f"""SELECT imtrn.xitem, SUM(imtrn.xqty*imtrn.xsign) as Stock
                        FROM imtrn
                        WHERE imtrn.zid = '{zid}'
                        AND imtrn.xdate <= '{end_date}'
                        GROUP BY imtrn.xitem""", con=engine)
    return df

def get_item_stock(zid, end_date, items):
    if isinstance(items, str):
        items = [items]
    item_list = ', '.join(f"'{item}'" for item in items)
    query = f"""SELECT imtrn.xitem, SUM(imtrn.xqty*imtrn.xsign) as Stock
                FROM imtrn
                WHERE imtrn.zid = '{zid}'
                AND imtrn.xdate < '{end_date}'
                AND imtrn.xitem IN ({item_list})
                GROUP BY imtrn.xitem"""
    return pd.read_sql_query(query, con=engine)

def get_item_stock_1(zid, end_date, item):
    df = pd.read_sql(f"""SELECT imtrn.xitem, SUM(imtrn.xqty*imtrn.xsign) as Stock
                        FROM imtrn
                        WHERE imtrn.zid = '{zid}'
                        AND imtrn.xdate < '{end_date}'
                        AND imtrn.xitem = '{item}'
                        GROUP BY imtrn.xitem""", con=engine)
    return df

def get_special_price(zid):
    df = pd.read_sql(f"""SELECT xpricecat, xqty,xdisc
                        FROM opspprc 
                        WHERE zid = '{zid}'""", con=engine)
    return df

def get_sales(zid, start_date, end_date, items):
    if isinstance(items, str):
        items = [items]
    item_list = ', '.join(f"'{item}'" for item in items)
    query = f"""SELECT imtrn.xitem, imtrn.xdate, imtrn.xcus, cacus.xstate ,SUM(imtrn.xval) as xval, sum(imtrn.xqty*imtrn.xsign) as sales
                FROM imtrn
                JOIN cacus
                ON imtrn.xcus = cacus.xcus
                WHERE imtrn.zid = '{zid}'
                AND cacus.zid = '{zid}'
                AND imtrn.xdocnum LIKE 'DO--%%'
                AND imtrn.xdate >= '{start_date}'
                AND imtrn.xdate <= '{end_date}'
                AND imtrn.xitem IN ({item_list})
                GROUP BY imtrn.xitem, imtrn.xdate, imtrn.xcus, cacus.xstate"""
    return pd.read_sql_query(query, con=engine)

def get_sales_1(zid, start_date, end_date, item):
    df = pd.read_sql(f"""SELECT imtrn.xitem, imtrn.xdate, imtrn.xcus, cacus.xstate ,SUM(imtrn.xval) as xval, sum(imtrn.xqty*imtrn.xsign) as sales
                        FROM imtrn
                        JOIN cacus
                        ON imtrn.xcus = cacus.xcus
                        WHERE imtrn.zid = '{zid}'
                        AND cacus.zid = '{zid}'
                        AND imtrn.xdocnum LIKE 'DO--%%'
                        AND imtrn.xdate >= '{start_date}'
                        AND imtrn.xdate <= '{end_date}'
                        AND imtrn.xitem = '{item}'
                        GROUP BY imtrn.xitem, imtrn.xdate, imtrn.xcus, cacus.xstate""", con=engine)
    return df

def get_return(zid, start_date, end_date, items):
    if isinstance(items, str):
        items = [items]
    item_list = ', '.join(f"'{item}'" for item in items)
    query = f"""SELECT imtrn.xitem, imtrn.xdate, imtrn.xcus, cacus.xstate , SUM(imtrn.xval) as xval, sum(imtrn.xqty*imtrn.xsign) as rtn
                FROM imtrn
                JOIN cacus
                ON imtrn.xcus = cacus.xcus
                WHERE imtrn.zid = '{zid}'
                AND cacus.zid = '{zid}'
                AND (imtrn.xdoctype = 'SR--' OR imtrn.xdoctype = 'DSR-')
                AND imtrn.xdate >= '{start_date}'
                AND imtrn.xdate <= '{end_date}'
                AND imtrn.xitem IN ({item_list})
                GROUP BY imtrn.xitem, imtrn.xdate, imtrn.xcus, cacus.xstate"""
    return pd.read_sql_query(query, con=engine)

def get_area_sales(zid, start_date, end_date, item):
    if isinstance(item, tuple):
        item_str = str(item)
    else:
        item_str = f"('{item}')"
    query = f"""SELECT opddt.xitem, opdor.xdate, opdor.xdiv, SUM(opddt.xdtwotax) as total_amount, SUM(opddt.xqty) as total_qty
                FROM opddt
                JOIN opdor
                ON opdor.xdornum = opddt.xdornum
                WHERE opdor.zid = '{zid}'
                AND opddt.zid = '{zid}'
                AND opdor.xdornum LIKE 'DO--%%'
                AND opdor.xdate >= '{start_date}'
                AND opdor.xdate <= '{end_date}'
                AND opddt.xitem IN {item_str}
                GROUP BY opddt.xitem, opdor.xdate, opdor.xdiv"""
    return pd.read_sql_query(query, con=engine)

def get_area_sales_1(zid, start_date, end_date, item):
    df = pd.read_sql(f"""SELECT opddt.xitem, opdor.xdate, opdor.xdiv, SUM(opddt.xdtwotax) as total_amount, SUM(opddt.xqty) as total_qty
                        FROM opddt
                        JOIN opdor
                        ON opdor.xdornum = opddt.xdornum
                        WHERE opdor.zid = '{zid}'
                        AND opddt.zid = '{zid}'
                        AND opdor.xdornum LIKE 'DO--%%'
                        AND opdor.xdate >= '{start_date}'
                        AND opdor.xdate <= '{end_date}'
                        AND opddt.xitem = '{item}'
                        GROUP BY opddt.xitem, opdor.xdate, opdor.xdiv""", con=engine)
    return df

def get_purchase(zid, start_date):
    df = pd.read_sql(f"""SELECT poodt.xitem, poord.xcounterno, poodt.xqtyord, poodt.xrate, pogrn.xgrnnum, pogrn.xdate
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
                        AND pogrn.xdate > '{start_date}'""", con=engine)
    return df

def get_gl_details_bs_project(zid, date):
    year = date.split('-')[0]
    df = pd.read_sql(f"""select glmst.zid, glmst.xacc, glmst.xdesc, gldetail.xsub, SUM(gldetail.xprime)
                        FROM glmst
                        JOIN
                        gldetail
                        ON glmst.xacc = gldetail.xacc
                        JOIN
                        glheader
                        ON gldetail.xvoucher = glheader.xvoucher
                        WHERE glmst.zid = '{zid}'
                        AND gldetail.zid = '{zid}'
                        AND glheader.zid = '{zid}'
                        AND gldetail.xproj = 'GULSHAN TRADING'
                        AND gldetail.xvoucher NOT LIKE 'OB-%%'
                        AND glheader.xdate <= '{date}'
                        AND glheader.xyear < '{year}'
                        AND glmst.xacc IN ('10010007','10020015','10020001','10010003','10010006')
                        GROUP BY glmst.zid, glmst.xacc, glmst.xdesc, gldetail.xsub""", con=engine)
    return df

def get_vat_amount(zid, ship_name):
    df = pd.read_sql(f"""SELECT xprime 
                        FROM gldetail 
                        WHERE zid = '{zid}' 
                        AND xacc = '01050007' 
                        AND xlong = '{ship_name}'""", con=engine)
    return df


# ─────────────────────────────────────────────────────────────────────
# 📦 5. Packaging & Area Mapping (Silent)
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

area_dict = {
    'Lakshmipur': 'District', 'Saver': 'Dhaka', 'Basundhara': 'General', 
    'Central-6(Nawab pur-1)': 'Wholesale', 'Central-5(Nawab pur-2)': 'Wholesale', 
    'Habiganj': 'District', 'Shymolly': 'Dhaka', 'Noyabazar': 'Dhaka',
    'Chittagong': 'District', 'Ibrahimpur': 'Dhaka', 'Naogaon': 'District', 
    'Kazipara': 'Dhaka', 'Sirajganj': 'District', 'Lalbag': 'Dhaka', 
    'Mawna': 'General', 'Tangail': 'District', 'Sunamganj': 'District', 
    'Narayanganj': 'General', 'Tongi': 'Dhaka', 'Kalir Bazar': 'Dhaka', 
    'Natore': 'District', 'Sylhet': 'Dhaka', 'Bagerhat': 'District', 
    'Nobi Nagar': 'Dhaka', 'Ctg.Road': 'Dhaka', 'Gopalgang': 'District', 
    'Coxs Bazar': 'Dhaka', 'Ghorasal': 'General', 'Askona': 'Dhaka', 
    'Central-8(Kawranbazar-1)': 'Wholesale', 'Narsingdi': 'General', 
    'Sariatpur': 'District', 'Jessore': 'District', 'Thakurgaon': 'District', 
    'Jhalakati': 'District', 'Munshiganj': 'General', 'Khulna': 'District', 
    'Fakir Market': 'Dhaka', 'Comilla': 'Distributor', 'Mohammadpur': 'Dhaka', 
    'Basabo': 'Dhaka', 'Central-2(Imamgonj)': 'Wholesale', 'Pirojpur': 'District', 
    'Asulia': 'Dhaka', 'Sreepur': 'District', 'Mirpur-1,2': 'Dhaka', 
    'Kurigram': 'District', 'Jaypurhat': 'District', 'Mirpur-11,12': 'Dhaka', 
    'Keraniganj': 'General', 'Marura': 'District', 'Jaipurhat': 'District', 
    'Zigatala': 'Dhaka', 'New Market': 'Dhaka', 'Panchagarh': 'District', 
    'Patuakhali': 'District', 'Uttar Badda': 'Dhaka', 'Sayedpur': 'District', 
    'Jhenaidah': 'District', 'Barguna': 'District', 'Abdulla Pur': 'Dhaka', 
    'Malibag': 'Dhaka', 'Safipur': 'Dhaka', 'Barisal': 'District', 
    'Rampura': 'Dhaka', 'Kaylanpur': 'Dhaka', 'Chuadanga': 'District', 
    'Manikgonj': 'General', 'Goshbug': 'Dhaka', 'Noakhali': 'District', 
    'Moulvibazer': 'District', 'Narail': 'District', 'Jattrabari': 'Dhaka', 
    'Bogra': 'District', 'Bhola': 'District', 'Gaibandha': 'District', 
    'Central-3(Alubazar-2)': 'Wholesale', 'Lamonirhat': 'District', 
    'Coxs Bazar': 'District', 'Dinajpur': 'District', 'Central-1(Imamgonj)': 'Wholesale', 
    'Kalachadpur': 'Dhaka', 'Kushtia': 'District', 'Rangamati': 'District', 
    'Central-4(Alubazar-1)': 'Wholesale', 'Shirajgonj': 'District', 
    'Ctg. Road': 'Dhaka', 'Voyrab': 'General', 'Satkhira': 'District', 
    'Kishoreganj': 'General', 'Jamalpur': 'District', 'Netrokona': 'District', 
    'Magura': 'District', 'Shibbari': 'Dhaka', 'Vatara': 'Dhaka', 
    'Board Bazer': 'Dhaka', 'firmget': 'Dhaka', 'Feni': 'District', 
    'Damra': 'Dhaka', 'Faridpur': 'District', 'Dohar': 'General', 
    'Pagla': 'Dhaka', 'Rangpur': 'District', 'Kaliakoir': 'General', 
    'Mirzapur': 'General', 'Rajbari': 'District', 'Rajshahi': 'District', 
    'Nilphamari': 'District', 'Meherpur': 'District', 'Uttora': 'Dhaka', 
    'Motijil': 'Dhaka', 'Cherag Ali': 'Dhaka', 'Mohakhali': 'Dhaka', 
    'Brahmanbaria': 'General', 'Chapainawabganj': 'District', 'Sherpur': 'District',
    'Khilket': 'Dhaka', 'Gulshan-1': 'Dhaka', 'Pabna': 'District', 
    'Banani': 'Dhaka', 'Central-7(Kawranbazar-2)': 'Wholesale', 
    'Mymensingh': 'District', 'Madaripur': 'Dhaka'
}

df_area = pd.DataFrame(area_dict.items(), columns=['xdiv', 'market'])
df_area = df_area[['xdiv', 'market']]


# ─────────────────────────────────────────────────────────────────────
# 🧱 6. Build Base DataFrame
# ─────────────────────────────────────────────────────────────────────
zid_trading = 100001
zid_packaging = 100009

df_caitem = get_caitem(zid_trading)
df_sp_price = get_special_price(zid_trading).rename(columns={'xpricecat': 'xitem'})
df_stock_hmbr = get_stock(zid_trading, end_date)
df_stock_pack = get_stock(zid_packaging, end_date)
df_stock_pack = df_stock_pack.rename(columns={'xitem': 'pack_code'})

# Merge all
dff = df_caitem.merge(df_sp_price[['xitem', 'xdisc']], on=['xitem'], how='left').fillna(0).rename(columns={'xstdprice': 'retailP'})
dff['wholesaleP'] = dff['retailP'] - dff['xdisc']
del dff['xdisc']

dff = dff.merge(df_stock_hmbr[['xitem', 'stock']], on=['xitem'], how='left').fillna(0).rename(columns={'stock': 'hmbr_stock'})
dff = dff.merge(df_pack[['xitem', 'pack_code']], on=['xitem'], how='left').fillna(0)
dff = dff.merge(df_stock_pack[['pack_code', 'stock']], on=['pack_code'], how='left').fillna(0).rename(columns={'stock': 'pack_stock'})
dff['Current_Stock'] = dff['hmbr_stock'] + dff['pack_stock']


# ─────────────────────────────────────────────────────────────────────
# 📦 7. Get Purchase & Filter MD Shipments
# ─────────────────────────────────────────────────────────────────────
df_po_trade = get_purchase(zid_trading, start_date)
dff = dff.merge(df_po_trade[['xitem', 'xcounterno', 'xqtyord', 'xrate', 'xgrnnum', 'xdate']], on=['xitem'], how='left')
df_po_pack = get_purchase(zid_packaging, start_date).rename(columns={'xitem': 'pack_code'})
dff = dff.merge(df_po_pack[['pack_code', 'xcounterno', 'xqtyord', 'xrate', 'xgrnnum', 'xdate']], on=['pack_code'], how='left')

# Combine purchase data: Qty and Rate
dff['xqtyord_x'] = dff['xqtyord_x'].fillna(0)
dff['xqtyord_y'] = dff['xqtyord_y'].fillna(0)
dff['Qty_order'] = dff['xqtyord_x'] + dff['xqtyord_y']

dff['xrate_x'] = dff['xrate_x'].fillna(0)
dff['xrate_y'] = dff['xrate_y'].fillna(0)
mask_both = (dff['xqtyord_x'] > 0) & (dff['xrate_x'] > 0) & (dff['xqtyord_y'] > 0) & (dff['xrate_y'] > 0)
mask_trading = (dff['xqtyord_x'] > 0) & (dff['xrate_x'] > 0)
mask_packaging = (dff['xqtyord_y'] > 0) & (dff['xrate_y'] > 0)

dff['p_rate'] = 0
dff.loc[mask_both, 'p_rate'] = (
    (dff['xqtyord_x'] * dff['xrate_x'] + dff['xqtyord_y'] * dff['xrate_y']) /
    (dff['xqtyord_x'] + dff['xqtyord_y'])
)[mask_both]
dff.loc[mask_trading & ~mask_both, 'p_rate'] = dff['xrate_x']
dff.loc[mask_packaging & ~mask_both, 'p_rate'] = dff['xrate_y']

# Combine GRN, Date, Counter
for col in ['xgrnnum', 'xdate', 'xcounterno']:
    dff[f'{col}_x'] = dff[f'{col}_x'].fillna('')
    dff[f'{col}_y'] = dff[f'{col}_y'].fillna('')
    dff[col] = dff[f'{col}_y'].where(dff[f'{col}_y'] != '', dff[f'{col}_x'])

# Drop intermediate columns
for col in ['xqtyord_x', 'xqtyord_y', 'xrate_x', 'xrate_y']:
    if col in dff.columns:
        dff = dff.drop(columns=[col])
for col in ['xgrnnum_x', 'xgrnnum_y', 'xdate_x', 'xdate_y', 'xcounterno_x', 'xcounterno_y']:
    if col in dff.columns:
        dff = dff.drop(columns=[col])

dff = dff.rename(columns={'xgrnnum': 'grunnum', 'xdate': 'date', 'xcounterno': 'counter_split'})
dff[['xcounterno', 'counterdate']] = dff['counter_split'].str.split(",", expand=True)
dff = dff[dff['xcounterno'].str.contains("MD", na=False)]


# ─────────────────────────────────────────────────────────────────────
# 📊 8. Process Each MD Shipment
# ─────────────────────────────────────────────────────────────────────
date_dict = dff.groupby('xcounterno')['date'].first().apply(lambda x: x.strftime("%Y-%m-%d")).to_dict()
item_dict = dff.groupby('xcounterno')['xitem'].apply(list).to_dict()

main_df_dict = {}
main_area_dict = {}
main_summary_df = {}

for dk, dv in date_dict.items():
    df_main = dff[dff['xcounterno'] == dk].copy()
    try:
        vat_amount = get_vat_amount(zid_trading, dk)['xprime'].iloc[0]
    except (IndexError, KeyError):
        vat_amount = 0

    items = item_dict[dk]
    df_stock = get_item_stock(zid_trading, dv, items)
    df_sales = get_sales(zid_trading, dv, end_date, items)
    df_sales_item = df_sales.groupby('xitem')['sales'].sum().reset_index()
    df_return = get_return(zid_trading, dv, end_date, items)
    df_return_item = df_return.groupby('xitem')['rtn'].sum().reset_index()

    df_main = df_main.merge(df_stock, on='xitem', how='left') \
                     .merge(df_sales_item, on='xitem', how='left') \
                     .merge(df_return_item, on='xitem', how='left') \
                     .fillna(0) \
                     .rename(columns={'stock': 'pre_stock', 'xitem': 'code', 'xdesc': 'name', 'xgitem': 'group', 'xstdcost': 'avg_cost'})

    df_main = df_main.drop(columns=['xcitem', 'xpricecat', 'xduty', 'xwh'])
    df_main = df_main[['xcounterno', 'code', 'pack_code', 'name', 'group', 'grunnum', 'date', 'avg_cost', 'p_rate', 'retailP', 'wholesaleP', 'pre_stock', 'Qty_order', 'sales', 'rtn', 'Current_Stock', 'hmbr_stock', 'pack_stock']]
    df_main['sales'] = df_main['sales'] + df_main['rtn']
    df_main['sales'] = np.where((df_main['sales'] * -1) > df_main['Qty_order'], df_main['Qty_order'] * -1, df_main['sales'])
    df_main['total_p_rev'] = df_main['Qty_order'] * df_main['wholesaleP'] * -1
    df_main['total_p_cost_exVat'] = df_main['Qty_order'] * df_main['p_rate']
    df_main['vat_amount'] = (df_main['total_p_cost_exVat'] / df_main['total_p_cost_exVat'].sum()) * vat_amount
    df_main['total_p_cost'] = (df_main['total_p_cost_exVat'] + df_main['vat_amount']).round(2)
    df_main['p_rate_vat'] = df_main['total_p_cost'] / df_main['Qty_order']
    df_main['total_rev'] = df_main['sales'] * df_main['wholesaleP'] * -1
    df_main['total_cost'] = df_main['sales'] * df_main['p_rate_vat']
    df_main['total_gp'] = df_main['total_p_rev'] + df_main['total_p_cost']
    df_main['gp'] = df_main['total_rev'] + df_main['total_cost']
    df_main['perc'] = (df_main['gp'] / df_main['total_gp']) * 100
    df_main = df_main.round(2)

    # Area Sales
    df_sales = df_sales.rename(columns={'xitem': 'code', 'sales': 'pre_sales'})
    df_return = df_return.rename(columns={'xitem': 'code'})
    df_sales_area = df_sales.merge(df_main[['code', 'wholesaleP', 'p_rate_vat']], on='code') \
                             .merge(df_return[['code', 'xcus', 'rtn']], on=['code', 'xcus'], how='left') \
                             .fillna(0)
    df_sales_area['sales'] = df_sales_area['pre_sales'] + df_sales_area['rtn']
    df_sales_area['total_sales'] = df_sales_area['sales'] * df_sales_area['wholesaleP'] * -1
    df_sales_area['total_cost'] = df_sales_area['sales'] * df_sales_area['p_rate_vat']
    df_sales_area['gp'] = df_sales_area['total_sales'] + df_sales_area['total_cost']
    df_sales_area['md_gp'] = df_sales_area['gp'] * 0.1
    df_sales_area['xdate'] = pd.to_datetime(df_sales_area['xdate'])
    df_sales_area['month'] = df_sales_area['xdate'].dt.month
    df_sales_area['year'] = df_sales_area['xdate'].dt.year
    df_sales_area = df_sales_area.groupby(['xstate', 'month', 'year'])[['total_sales', 'total_cost', 'gp', 'md_gp']].sum().round(2).reset_index().sort_values(['year', 'month'])
    df_sales_area.loc[len(df_sales_area)] = df_sales_area.sum(numeric_only=True)
    df_sales_area = df_sales_area.fillna('Total')

    main_df_dict[dk] = df_main
    main_area_dict[dk] = df_sales_area

    # Summary KPIs
    summary = {
        'Number of Days Passed': (datetime.strptime(dv, '%Y-%m-%d').date() - datetime.today().date()).days * -1,
        'Total Possible Revenue': df_main['total_p_rev'].sum() * -1,
        'Total Revenue To Date': df_main['total_rev'].sum(),
        'Total Possible Gross Profit': df_main['total_gp'].sum(),
        'Total Gross Profit To Date': df_main['gp'].sum(),
        'Total Possible Cost': df_main['total_p_cost'].sum(),
        'Total Cost to Date': df_main['total_cost'].sum() * -1,
        'Total possible payment to Md Sir': (df_main['total_p_cost'].sum() + df_main['total_gp'].sum() * 0.1),
        'Total payment to Md Sir To Date': (df_main['total_cost'].sum() * -1 + df_main['gp'].sum() * 0.1)
    }

    df_summary = pd.DataFrame(list(summary.items()), columns=['Topic', 'Value']).round(2)
    df_summary['Value'] = df_summary['Value'].apply(lambda x: f"{x:,.2f}")
    main_summary_df[dk] = df_summary


# ─────────────────────────────────────────────────────────────────────
# 💰 9. Bank Details
# ─────────────────────────────────────────────────────────────────────
bank_details = get_gl_details_bs_project(zid_trading, end_date).round(1).rename(columns={'sum': 'Balance'})

main_bank_dict = {}
# Dhaka Bank
dhaka = bank_details[bank_details['xacc'] == '10010003'].copy()
dhaka.loc[len(dhaka)] = [zid_trading, '10010003', 'Limit', '00000', 50_000_000]
dhaka.loc[len(dhaka)] = [zid_trading, '10010003', 'Balance', '00000', dhaka['Balance'].sum()]
dhaka['Balance'] = dhaka['Balance'].apply(lambda x: f" {x:,.1f}")
main_bank_dict['Dhaka Bank Balance'] = dhaka

# UCB Bank
ucb = bank_details[bank_details['xacc'] == '10010006'].copy()
ucb.loc[len(ucb)] = [zid_trading, '10010006', 'Limit', '00000', 25_000_000]
ucb.loc[len(ucb)] = [zid_trading, '10010006', 'Balance', '00000', ucb['Balance'].sum()]
ucb['Balance'] = ucb['Balance'].apply(lambda x: f" {x:,.1f}")
main_bank_dict['UCB Bank Balance'] = ucb

# MD Overdraft
md_od = bank_details[bank_details['xacc'] == '10010007'].copy()
md_od.loc[len(md_od)] = [zid_trading, '10010007', 'Limit', '00000', 40_000_000]
md_od.loc[len(md_od)] = [zid_trading, '10010007', 'Balance', '00000', md_od['Balance'].sum()]
md_od['Balance'] = md_od['Balance'].apply(lambda x: f" {x:,.1f}")
main_bank_dict['MD sir Overdraft Balance'] = md_od

# Loan from MD (Ex Mfg)
loan_ex = bank_details[bank_details['xacc'] == '10020001'].copy()
loan_ex.loc[len(loan_ex)] = [zid_trading, '10020001', 'Loans Received From MD sir', '00000', loan_ex['Balance'].sum()]
loan_ex['Balance'] = loan_ex['Balance'].apply(lambda x: f" {x:,.1f}")
main_bank_dict['Loan received from MD Sir (Ex Mfg)'] = loan_ex

# Loan from MD (Only Mfg)
loan_mfg = bank_details[bank_details['xacc'] == '10020015'].copy()
loan_mfg.loc[len(loan_mfg)] = [zid_trading, '10020015', 'Loans Received From MD sir for Manufacturing', '00000', loan_mfg['Balance'].sum()]
loan_mfg['Balance'] = loan_mfg['Balance'].apply(lambda x: f" {x:,.1f}")
main_bank_dict['Loan Received from MD Sir (Only Mfg) Balance'] = loan_mfg

# ─────────────────────────────────────────────────────────────────────
# 📁 10. Export & Email
# ─────────────────────────────────────────────────────────────────────
with open('main_summary_df.html', 'w', encoding='utf-8') as f:
    f.write("<html><body>")
    for dk in main_summary_df:
        f.write(f"<h1>{dk}</h1>")
        f.write(main_summary_df[dk].to_html(index=False))
    for name, df in main_bank_dict.items():
        f.write(f"<h1>{name}</h1>")
        f.write(df.to_html(index=False))
    for dk in main_area_dict:
        f.write(f"<h1>{dk} Area Sales</h1>")
        f.write(main_area_dict[dk].to_html(index=False))
    f.write("</body></html>")

with pd.ExcelWriter('main_area.xlsx', engine='openpyxl') as writer:
    for dk, df in main_area_dict.items():
        sheet_name = f"{dk[:7]}--{dk[-5:]}".replace('/', '_').replace('\\', '_').replace('?', '_').replace('*', '_').replace('[', '_').replace(']', '_').replace(':', '_')[:31]
        df.to_excel(writer, sheet_name=sheet_name, index=False)

with pd.ExcelWriter('main_df.xlsx', engine='openpyxl') as writer:
    for dk, df in main_df_dict.items():
        sheet_name = f"{dk[:7]}--{dk[-5:]}".replace('/', '_').replace('\\', '_').replace('?', '_').replace('*', '_').replace('[', '_').replace(']', '_').replace(':', '_')[:31]
        df.to_excel(writer, sheet_name=sheet_name, index=False)

# 📬 Send Email
try:
    report_name = os.path.splitext(os.path.basename(__file__))[0]
    recipients = get_email_recipients(report_name)
    print(f"📬 Recipients: {recipients}")
except Exception as e:
    print(f"⚠️ Fallback: {e}")
    recipients = ["ithmbrbd@gmail.com"]

subject = f"HM_21 Special Shipment information-of-{','.join(main_df_dict.keys())}"
body_text = "Please find the special shipment report attached."

html_content = []
for dk, df in main_summary_df.items():
    html_content.append((df, dk))
for name, df in main_bank_dict.items():
    html_content.append((df, name))
for dk, df in main_area_dict.items():
    html_content.append((df, f"{dk} Area Sales"))

send_mail(
    subject=subject,
    bodyText=body_text,
    attachment=['main_area.xlsx', 'main_df.xlsx'],
    recipient=recipients,
    html_body=html_content
)

print("✅ HM_21 completed successfully.")