"""
📊 H_15_2_Zepto_Sales_Analytics.py – Monthly Sales Analytics for Zepto Chemicals

🚀 PURPOSE:
    - Replicate legacy Zepto sales report with original logic
    - Generate pivot tables: salesman, area, customer, item
    - Highlight top/bottom performers
    - Export to two Excel files
    - Send HTML email with vertical summary

📁 OUTPUT:
    - H_15_2_Zepto_Sales_Information.xlsx
    - H_15_2_Zepto_Sales_Ratios.xlsx
    - Email with HTML summary (SL No., vertical layout)

📬 EMAIL:
    - Recipients: get_email_recipients("H_15_2_Zepto_Sales_Analytics")
    - Fallback: ithmbrbd@gmail.com, zepto.sales1@gmail.com
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


# === 1. Load Environment Variables from .env ===
load_dotenv()

try:
    ZID = int(os.environ["ZID_ZEPTO_CHEMICALS"])
except KeyError:
    raise RuntimeError("❌ ZID_ZEPTO_CHEMICALS not found in .env")

print(f"🏢 Processing for ZID={ZID} (Zepto Chemicals)")


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
thisYear = datetime.now().year
thisMonth = datetime.now().month
thisDay = datetime.now().day
thisDayName = datetime.now().strftime("%A")

# Adjust month logic
if (thisDay == 2 and thisDayName == 'Saturday') or thisDay == 1:
    analysis_month = thisMonth - 1 if thisMonth != 1 else 12
else:
    analysis_month = thisMonth

print(f"📅 Analysis Month: {analysis_month}")


# === 7. Fetch Master Data (Single Engine) ===
def create_prmst(zid):
    return pd.read_sql("SELECT xemp, xname FROM prmst WHERE zid = %(zid)s", engine, params={'zid': zid})

def create_cacus(zid):
    return pd.read_sql("SELECT zid, xcus, xshort, xadd2, xcity FROM cacus WHERE zid = %(zid)s", engine, params={'zid': zid})

def create_caitem(zid):
    return pd.read_sql("SELECT zid, xitem, xdesc, xgitem, xstdprice, xsrate FROM caitem WHERE zid = %(zid)s", engine, params={'zid': zid})

def create_opord(zid):
    return pd.read_sql("SELECT xordernum, xdate, xcus, xdiv, xsp, xtotamt FROM opord WHERE zid = %(zid)s", engine, params={'zid': zid})

def create_opodt(zid):
    return pd.read_sql("SELECT zid, xordernum, xitem, xqtydel, xrate, xdisc, xdiscf, xlineamt, xdtwotax, xdtdisc, xdtcomm FROM opodt WHERE zid = %(zid)s", engine, params={'zid': zid})

def create_opcrn(zid):
    return pd.read_sql("SELECT xcrnnum, xdate, xcus, xdisc, xdiscf, xglref, xordernum, xemp FROM opcrn WHERE zid = %(zid)s", engine, params={'zid': zid})

def create_opcdt(zid):
    return pd.read_sql("SELECT zid, xcrnnum, xitem, xqty, xdornum, xrate, xlineamt FROM opcdt WHERE zid = %(zid)s", engine, params={'zid': zid})

def create_rectreturn(zid):
    df_trn = pd.read_sql("""
        SELECT zid, ximtmptrn, xdate, xyear, xper, xcus, xemp, xarea, xtrnimf 
        FROM imtemptrn 
        WHERE xstatustrn = '5-Confirmed' AND zid = %(zid)s
    """, engine, params={'zid': zid})
    df_dtl = pd.read_sql("SELECT ximtmptrn, xitem, xqtyord, xrate, xlineamt FROM imtemptdt WHERE zid = %(zid)s", engine, params={'zid': zid})
    df = pd.merge(df_dtl, df_trn, on='ximtmptrn', how='left')
    df = df[df['xtrnimf'] == 'RECT']
    df = df[df['xyear'] == thisYear]
    return df


# === 8. Build Main Dataset ===
df_prmst = create_prmst(ZID)
df_cacus = create_cacus(ZID)
df_caitem = create_caitem(ZID)
df_opord = create_opord(ZID)
df_opodt = create_opodt(ZID)
df_opcrn = create_opcrn(ZID)
df_opcdt = create_opcdt(ZID)

df_main_sale = pd.merge(df_opodt, df_opord, on='xordernum', how='left')
df_main_return = pd.merge(df_opcdt, df_opcrn, on='xcrnnum', how='left')
df_main_return = df_main_return.rename(columns={
    'zid': 'zidreturn', 'xrate': 'xratereturn', 'xlineamt': 'xlineamtreturn',
    'xdisc': 'xdiscreturn', 'xdiscf': 'xdiscfreturn', 'xcus': 'xcusreturn',
    'xdate': 'xdatereturn', 'xqty': 'xqtyreturn'
})
df_main = pd.merge(df_main_sale, df_main_return, on=['xordernum', 'xitem'], how='left')
df_main = pd.merge(df_main, df_cacus, on='xcus', how='left')
df_main = pd.merge(df_main, df_caitem, on='xitem', how='left')
df_main = pd.merge(df_main, df_prmst.rename(columns={'xemp': 'xsp'}), on='xsp', how='left')
df_main = df_main.fillna(0)
df_main['xfinallineamt'] = df_main['xlineamt'] - df_main['xlineamtreturn']
df_main['xfinalqtydel'] = df_main['xqtydel'] - df_main['xqtyreturn']  # Note: xqtydel from opodt
df_main['xfinalrate'] = df_main['xfinallineamt'] / df_main['xfinalqtydel'].replace(0, 1)
df_main = df_main.drop(['zid_x', 'zid_y'], axis=1, errors='ignore')
df_main['xdate'] = pd.to_datetime(df_main['xdate'])
df_main['Year'] = df_main['xdate'].dt.year
df_main['Month'] = df_main['xdate'].dt.month
df_main = df_main[df_main['Year'] == thisYear]


# === 9. Generate Reports (Your Logic) ===

# Salesman-wise product sales (YTD)
df_salesman_product = df_main.groupby(['xsp', 'xname', 'xitem', 'xdesc'])['xfinalqtydel'].sum().reset_index()
df_salesman_product = df_salesman_product[df_salesman_product['xname'] != 0]
df_salesman_product['spname'] = df_salesman_product['xsp'] + ':-' + df_salesman_product['xname']
df_salesman_product['itemdesc'] = df_salesman_product['xitem'] + ':-' + df_salesman_product['xdesc']
df_salesman_product = df_salesman_product.pivot(index='spname', columns='itemdesc', values='xfinalqtydel').fillna(0)
df_salesman_product = df_salesman_product.reset_index().rename_axis(None, axis=1)
df_salesman_product = df_salesman_product.rename(columns={'spname': 'Salesman Name'})
df_salesman_product.loc['sum'] = df_salesman_product.sum(numeric_only=True)
df_salesman_product = df_salesman_product.fillna(0)
df_salesman_product.loc['sum', 'Salesman Name'] = 'Total'

# This month product sales
df_main_month = df_main[df_main['Month'] == analysis_month]
df_salesman_product_month = df_main_month.groupby(['xsp', 'xname', 'xitem', 'xdesc'])['xfinalqtydel'].sum().reset_index()
df_salesman_product_month = df_salesman_product_month[df_salesman_product_month['xname'] != 0]
df_salesman_product_month['spname'] = df_salesman_product_month['xsp'] + ':-' + df_salesman_product_month['xname']
df_salesman_product_month['itemdesc'] = df_salesman_product_month['xitem'] + ':-' + df_salesman_product_month['xdesc']
df_salesman_product_month = df_salesman_product_month.pivot(index='spname', columns='itemdesc', values='xfinalqtydel').fillna(0)
df_salesman_product_month = df_salesman_product_month.reset_index().rename_axis(None, axis=1)
df_salesman_product_month = df_salesman_product_month.rename(columns={'spname': 'Salesman Name'})
df_salesman_product_month.loc['sum'] = df_salesman_product_month.sum(numeric_only=True)
df_salesman_product_month = df_salesman_product_month.fillna(0)
df_salesman_product_month.loc['sum', 'Salesman Name'] = 'Total'

# Date-wise product sales
df_datewise_product = df_main.groupby(['xdate', 'xitem', 'xdesc'])['xfinalqtydel'].sum().reset_index()
df_datewise_product['itemdesc'] = df_datewise_product['xitem'] + df_datewise_product['xdesc']
df_datewise_product = df_datewise_product.pivot(index='xdate', columns='itemdesc', values='xfinalqtydel').fillna(0)
df_datewise_product = df_datewise_product.reset_index().rename_axis(None, axis=1)

# Area-wise product sales
df_areawise_product = df_main.groupby(['xdiv', 'xitem', 'xdesc'])['xfinalqtydel'].sum().reset_index()
df_areawise_product['itemdesc'] = df_areawise_product['xitem'] + ':-' + df_areawise_product['xdesc']
df_areawise_product = df_areawise_product.pivot(index='xdiv', columns='itemdesc', values='xfinalqtydel').fillna(0)
df_areawise_product = df_areawise_product.reset_index().rename_axis(None, axis=1)
df_areawise_product = df_areawise_product.rename(columns={'xdiv': 'Area'})
df_areawise_product.loc['sum'] = df_areawise_product.sum(numeric_only=True)
df_areawise_product = df_areawise_product.fillna(0)
df_areawise_product.loc['sum', 'Area'] = 'Total'

# Customer per Area product sales
df_customer_area_product = df_main.groupby(['xdiv', 'xcus', 'xshort', 'xitem', 'xdesc'])['xfinalqtydel'].sum().reset_index()
df_customer_area_product = pd.pivot_table(
    df_customer_area_product,
    index=['xdiv', 'xcus', 'xshort'],
    columns=['xitem', 'xdesc'],
    aggfunc='sum'
).fillna(0)


# === 10. Summary Metrics (Your Logic) ===
infoDictMonthly = {}
df_month = df_main[df_main['Month'] == analysis_month]

# Gross Sales
gs = df_month.groupby(['xsp', 'xname'])['xfinallineamt'].sum()
infoDictMonthly['Salesman with Highest Gross Sales'] = list(gs.idxmax()) + [gs.max()]
infoDictMonthly['Salesman with Lowest Gross Sales'] = list(gs.idxmin()) + [gs.min()]

ga = df_month.groupby('xdiv')['xfinallineamt'].sum()
infoDictMonthly['Area with Highest Gross Sales'] = [ga.idxmax(), ga.max()]
infoDictMonthly['Area with Lowest Gross Sales'] = [ga.idxmin(), ga.min()]

gc = df_month.groupby(['xcus', 'xshort', 'xdiv'])['xfinallineamt'].sum()
infoDictMonthly['Customer with Highest Gross Sales'] = list(gc.idxmax()) + [gc.max()]
infoDictMonthly['Customer with Lowest Gross Sales'] = list(gc.idxmin()) + [gc.min()]

gi = df_month.groupby(['xitem', 'xdesc'])['xfinallineamt'].sum()
infoDictMonthly['Item with Highest Gross Sales'] = list(gi.idxmax()) + [gi.max()]
infoDictMonthly['Item with Lowest Gross Sales'] = list(gi.idxmin()) + [gi.min()]

# Units Sold
uss = df_month.groupby(['xsp', 'xname'])['xfinalqtydel'].sum()
infoDictMonthly['Salesman with Highest Unit Sold'] = list(uss.idxmax()) + [uss.max()]
infoDictMonthly['Salesman with Lowest Unit Sold'] = list(uss.idxmin()) + [uss.min()]

usa = df_month.groupby('xdiv')['xfinalqtydel'].sum()
infoDictMonthly['Area with Highest Unit Sold'] = [usa.idxmax(), usa.max()]
infoDictMonthly['Area with Lowest Unit Sold'] = [usa.idxmin(), usa.min()]

usc = df_month.groupby(['xcus', 'xshort', 'xdiv'])['xfinalqtydel'].sum()
infoDictMonthly['Customer who bought Highest Units'] = list(usc.idxmax()) + [usc.max()]
infoDictMonthly['Customer who bought Lowest Units'] = list(usc.idxmin()) + [usc.min()]

usi = df_month.groupby(['xitem', 'xdesc'])['xfinalqtydel'].sum()
infoDictMonthly['Item which had the Highest Units Sold'] = list(usi.idxmax()) + [usi.max()]
infoDictMonthly['Item which had the Lowest Units Sold'] = list(usi.idxmin()) + [usi.min()]

# Orders
ocs = df_month.groupby(['xsp', 'xname'])['xordernum'].nunique()
infoDictMonthly['Salesman with the Highest Number of Orders'] = list(ocs.idxmax()) + [ocs.max()]
infoDictMonthly['Salesman with the Lowest Number of Orders'] = list(ocs.idxmin()) + [ocs.min()]
infoDictMonthly['Average Order Per Salesman'] = round(ocs.mean(), 2)

occ = df_month.groupby(['xcus', 'xshort'])['xordernum'].nunique()
infoDictMonthly['Customer who gave the Highest number of Orders'] = list(occ.idxmax()) + [occ.max()]
infoDictMonthly['Customer who gave the Lowest number of Orders'] = list(occ.idxmin()) + [occ.min()]
infoDictMonthly['Average Order Per Customer'] = round(occ.mean(), 2)

oca = df_month.groupby('xdiv')['xordernum'].nunique()
infoDictMonthly['Area with the Highest Number of Orders'] = [oca.idxmax(), oca.max()]
infoDictMonthly['Area with the Lowest Number of Orders'] = [oca.idxmin(), oca.min()]
infoDictMonthly['Average Order Per Area'] = round(oca.mean(), 2)

oci = df_month.groupby(['xitem', 'xdesc'])['xordernum'].nunique()
infoDictMonthly['Items with the Highest Number of Orders'] = list(oci.idxmax()) + [oci.max()]
infoDictMonthly['Item with the Lowest Number of Orders'] = list(oci.idxmin()) + [oci.min()]
infoDictMonthly['Average Order Per Item'] = round(oci.mean(), 2)

# Customer Count
cca = df_month.groupby('xdiv')['xcus'].nunique()
infoDictMonthly['Area with the Highest Number of Customers'] = [cca.idxmax(), cca.max()]
infoDictMonthly['Area with the Lowest Number of Customers'] = [cca.idxmin(), cca.min()]

ccs = df_month.groupby(['xsp', 'xname'])['xcus'].nunique()
infoDictMonthly['Salesman with the Highest Number of Customers'] = [ccs.idxmax(), ccs.max()]
infoDictMonthly['Salesman with the Lowest Number of Customers'] = [ccs.idxmin(), ccs.min()]

cci = df_month.groupby(['xitem', 'xdesc'])['xcus'].nunique()
infoDictMonthly['Items that were Distributed the Most'] = [cci.idxmax(), cci.max()]
infoDictMonthly['Items that were Distributed the Least'] = [cci.idxmin(), cci.min()]

# Totals
infoDictMonthly['Total Number of Orders'] = df_month['xordernum'].nunique()
infoDictMonthly['Total Number of Customers'] = df_month['xcus'].nunique()
infoDictMonthly['Total Units Sold this Month'] = df_month['xfinalqtydel'].sum()
infoDictMonthly['Total Amount Earned this Month'] = df_month['xfinallineamt'].sum()


# === 11. Convert Summary to DataFrame (Vertical, SL No.) ===
dict_df = pd.DataFrame({key: pd.Series(value) for key, value in infoDictMonthly.items()})
dict_df = pd.melt(dict_df, var_name='All_Info').reset_index()
dict_df = dict_df.groupby('All_Info')['value'].apply(list).to_frame().reset_index()
dict_df[[0, 1, 2, 3]] = pd.DataFrame(dict_df.value.tolist(), index=dict_df.index)
dict_df = dict_df.drop('value', axis=1)
dict_df = dict_df.fillna(0)
dict_df.insert(0, 'SL', range(1, len(dict_df) + 1))
cols = ['SL', 'All_Info'] + [c for c in [0, 1, 2, 3] if c in dict_df.columns]
dict_df = dict_df[cols]


# === 12. Export to Excel ===
INFO_FILE = "H_15_2_Zepto_Sales_Information.xlsx"
RATIOS_FILE = "H_15_2_Zepto_Sales_Ratios.xlsx"

with pd.ExcelWriter(INFO_FILE, engine='openpyxl') as writer:
    df_salesman_product.to_excel(writer, sheet_name='Salesman_Product_Sales', index=False)
    df_salesman_product_month.to_excel(writer, sheet_name='Salesman_Product_Month', index=False)
    df_datewise_product.to_excel(writer, sheet_name='Datewise_Product_Sales', index=False)
    df_areawise_product.to_excel(writer, sheet_name='Areawise_Product_Sales', index=False)
    df_customer_area_product.to_excel(writer, sheet_name='Customer_perArea_Product_Sales')

with pd.ExcelWriter(RATIOS_FILE, engine='openpyxl') as writer:
    gs.to_excel(writer, sheet_name='Gross_Sales_Salesman')
    ga.to_excel(writer, sheet_name='Gross_Sales_Area')
    gc.to_excel(writer, sheet_name='Gross_Sales_Customer')
    gi.to_excel(writer, sheet_name='Gross_Sales_Item')
    uss.to_excel(writer, sheet_name='Units_Sold_Salesman')
    usa.to_excel(writer, sheet_name='Units_Sold_Area')
    usc.to_excel(writer, sheet_name='Units_Sold_Customer')
    usi.to_excel(writer, sheet_name='Units_Sold_Item')
    ocs.to_excel(writer, sheet_name='Orders_Salesman')
    occ.to_excel(writer, sheet_name='Orders_Customer')
    oca.to_excel(writer, sheet_name='Orders_Area')
    oci.to_excel(writer, sheet_name='Orders_Item')
    cca.to_excel(writer, sheet_name='Customers_Area')
    ccs.to_excel(writer, sheet_name='Customers_Salesman')
    cci.to_excel(writer, sheet_name='Customers_Item')
    ipa = df_month.groupby(['xdiv', 'xitem', 'xdesc'])[['xfinalqtydel']].sum()
    ipa.to_excel(writer, sheet_name='Items_Sold_Per_Area')
    dict_df.to_excel(writer, sheet_name='Overall_Summary', index=False)

    # Rect returns
    df_rectreturn = create_rectreturn(ZID)
    df_rectreturn = df_rectreturn.merge(df_prmst.rename(columns={'xemp': 'xemp'}), on='xemp', how='left')
    df_rectreturn = df_rectreturn.merge(df_caitem, on='xitem', how='left')
    df_rectreturn = df_rectreturn[df_rectreturn['xper'] == analysis_month]
    rectsalesman = df_rectreturn.groupby(['xemp', 'xname'])[['xlineamt']].sum()
    rectsalesmanarea = df_rectreturn.groupby(['xemp', 'xname', 'xarea'])[['xlineamt']].sum()
    rectarea = df_rectreturn.groupby(['xarea'])[['xlineamt']].sum()
    rectproductamt = df_rectreturn.groupby(['xitem', 'xdesc'])[['xlineamt']].sum()
    rectproductqty = df_rectreturn.groupby(['xitem', 'xdesc'])[['xqtyord']].sum()

    # Safe write (avoid empty MultiIndex crash)
    def safe_to_excel(df, sheet_name):
        if not df.empty:
            df.to_excel(writer, sheet_name=sheet_name)
        else:
            pd.DataFrame({"Status": [f"No data for {sheet_name}"]}).to_excel(writer, sheet_name=sheet_name, index=False)

    safe_to_excel(rectsalesman, 'Rect_Salesman')
    safe_to_excel(rectsalesmanarea, 'Rect_Salesman_Area')
    safe_to_excel(rectarea, 'Rect_Area')
    safe_to_excel(rectproductamt, 'Rect_Product_Amt')
    safe_to_excel(rectproductqty, 'Rect_Product_Qty')


# === 13. Send Email ===
try:
    recipients = get_email_recipients(os.path.splitext(os.path.basename(__file__))[0])
    print(f"📬 Recipients: {recipients}")
except Exception as e:
    print(f"⚠️ Fallback: {e}")
    recipients = ["ithmbrbd@gmail.com", "zepto.sales1@gmail.com"]

subject = f"H_15.2 Zepto Sales Report – {analysis_month:02d}-{thisYear}"
body_text = "Please find the monthly sales reports for Zepto Chemicals attached."

send_mail(
    subject=subject,
    bodyText=body_text,
    attachment=[INFO_FILE, RATIOS_FILE],
    recipient=recipients,
    html_body=[(dict_df, "Monthly Performance Summary")]
)

print("✅ H_15.2 completed successfully.")