"""
📦 HM_25_Zepto_Overall_Details.py – Zepto Business Summary Report

🚀 PURPOSE:
    - Generate comprehensive business report for Zepto
    - Includes customer sales, AR, AP, item sales, purchase
    - Summarize by business unit (Zepto, HMBR, Fixit, E-Commerce, etc.)

🏢 AFFECTED BUSINESS:
    - Zepto Chemicals (ZID=100005)
    - Data Source: PostgreSQL (localhost:5432/da)

📅 PERIOD:
    - Last 6 months of data
    - Dynamic month range based on current date

📁 OUTPUT:
    - HM_25_Zepto_Overall_Details.xlsx → Multi-sheet Excel
    - index.html → HTML summary for email
    - Email with full HTML body and attachment

📬 EMAIL:
    - Sent via raw SMTP (same as original)
    - HTML body includes 5 tables with red headers
    - Recipients: ithmbrbd@gmail.com
    - Attachment: HM_25_Zepto_Overall_Details.xlsx

🔧 ENHANCEMENTS:
    - Uses project_config.DATABASE_URL
    - HM_25 prefix on Excel file
    - Better comments and documentation
    - No logic or flow changes
    - One-line cell documentation at the end
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
import psycopg2
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
from dotenv import load_dotenv

# ─────────────────────────────────────────────────────────────────────
# 1. Load Environment & Setup
# ─────────────────────────────────────────────────────────────────────
load_dotenv()

# Load ZID from .env
try:
    ZID_ZEPTO = int(os.environ["ZID_ZEPTO_CHEMICALS"])
except KeyError as e:
    raise RuntimeError(f"❌ Missing ZID in .env: {e}")


# Add root to sys.path to import mail
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
# ─────────────────────────────────────────────────────────────────────
# Data Fetch Functions (No structural changes — same as original)
# ─────────────────────────────────────────────────────────────────────

# Load DATABASE_URL from project_config
from project_config import DATABASE_URL
from mail import send_mail, get_email_recipients

engine = create_engine(DATABASE_URL)


def get_cus(zid):
    df = pd.read_sql("""SELECT cacus.xcus,cacus.xshort,cacus.xadd2, cacus.xcity,cacus.xstate FROM cacus WHERE zid = '%s'""" % (zid), con=engine)
    return df

def get_item(zid):
    df = pd.read_sql("""SELECT caitem.xitem,caitem.xdesc FROM caitem WHERE zid = '%s' AND xgitem = 'Industrial & Household'""" % (zid), con=engine)
    return df

def get_sales(zid, year, month):
    date = str(year) + '-' + str(month) + '-' + '01'
    df = pd.read_sql("""SELECT imtrn.xcus,imtrn.xitem, imtrn.xyear, imtrn.xper, imtrn.xdate , imtrn.xqty,
     imtrn.xdoctype ,imtrn.xdocnum, opddt.xrate , (opddt.xdtwotax - opddt.xdtdisc) as xdtwotax,  opdor.xdiscamt, opdor.xtotamt, opdor.xsp
                    FROM imtrn
                    JOIN opddt
                    ON (imtrn.xdocnum = opddt.xdornum AND imtrn.xitem = opddt.xitem AND imtrn.xdocrow = opddt.xrow)
                    JOIN opdor
                    ON imtrn.xdocnum = opdor.xdornum
                    WHERE imtrn.zid = '%s'
                    AND opddt.zid = '%s'
                    AND opdor.zid = '%s'
                    AND imtrn.xdate >= '%s'
                    AND imtrn.xdoctype = '%s'""" % (zid, zid, zid, date, 'DO--'), con=engine)
    return df

def get_return(zid, year, month):
    date = str(year) + '-' + str(month) + '-' + '01'
    df = pd.read_sql("""SELECT imtrn.xcus, imtrn.xitem, imtrn.xyear, imtrn.xper, imtrn.xdate,imtrn.xqty, opcdt.xrate, (opcdt.xrate*imtrn.xqty) as totamt, imtrn.xdoctype ,imtrn.xdocnum, opcrn.xemp
                        FROM imtrn 
                        JOIN opcdt
                        ON imtrn.xdocnum = opcdt.xcrnnum
                        AND imtrn.xitem = opcdt.xitem
                        JOIN opcrn
                        ON imtrn.xdocnum = opcrn.xcrnnum
                        WHERE imtrn.zid = '%s'
                        AND opcdt.zid = '%s'
                        AND opcrn.zid = '%s'
                        AND imtrn.xdate >= '%s'
                        AND imtrn.xdoctype = '%s'""" % (zid, zid, zid, date, 'SR--'), con=engine)
    return df

def get_acc_receivable(zid, proj, year, month):
    year_month = str(year) + str(month)
    df = pd.read_sql("""SELECT gldetail.xsub,cacus.xshort,cacus.xadd2,cacus.xcity,cacus.xstate,SUM(gldetail.xprime) as AR
                        FROM glheader
                        JOIN gldetail
                        ON glheader.xvoucher = gldetail.xvoucher
                        JOIN cacus
                        ON gldetail.xsub = cacus.xcus
                        WHERE glheader.zid = '%s'
                        AND gldetail.zid = '%s'
                        AND cacus.zid = '%s'
                        AND gldetail.xproj = '%s'
                        AND gldetail.xvoucher NOT LIKE '%s'
                        AND CONCAT(glheader.xyear,glheader.xper) <= '%s'
                        GROUP BY gldetail.xsub,cacus.xshort,cacus.xadd2,cacus.xcity,cacus.xstate""" % (zid, zid, zid, proj, 'OB--%%', year_month), con=engine)
    return df

def get_acc_payable(zid, proj, year, month):
    year_month = str(year) + str(month)
    df = pd.read_sql("""SELECT gldetail.xsub,casup.xshort,SUM(gldetail.xprime) as AP
                        FROM glheader
                        JOIN gldetail
                        ON glheader.xvoucher = gldetail.xvoucher
                        JOIN casup
                        ON gldetail.xsub = casup.xsup
                        WHERE glheader.zid = '%s'
                        AND gldetail.zid = '%s'
                        AND casup.zid = '%s'
                        AND gldetail.xproj = '%s'
                        AND gldetail.xvoucher NOT LIKE '%s'
                        AND CONCAT(glheader.xyear,glheader.xper) <= '%s'
                        GROUP BY gldetail.xsub,casup.xshort""" % (zid, zid, zid, proj, 'OB--%%', year_month), con=engine)
    return df

def get_employee(zid):
    df = pd.read_sql("""SELECT xemp,xname,xdept,xdesig,xstatusemp FROM prmst WHERE zid = '%s'""" % (zid), con=engine)
    return df

def get_purchase(zid, year, month):
    date = str(year) + '-' + str(month) + '-' + '01'
    df = pd.read_sql("""SELECT imtrn.zid, imtrn.xitem, imtrn.xyear, imtrn.xper, caitem.xdesc,caitem.xgitem, SUM(imtrn.xval) AS Purchase
                                    FROM imtrn
                                    JOIN caitem
                                    ON imtrn.xitem = caitem.xitem
                                    WHERE imtrn.zid = %s
                                    AND caitem.zid = %s
                                    AND imtrn.xdocnum LIKE '%s'
                                    AND imtrn.xdate > '%s'
                                    GROUP BY imtrn.zid, imtrn.xitem, imtrn.xyear, imtrn.xper, caitem.xdesc, caitem.xgitem, caitem.xstdprice""" % (zid, zid, 'GRN-%%', date), con=engine)
    return df


# ─────────────────────────────────────────────────────────────────────
# Main Logic (100% same to same — no changes)
# ─────────────────────────────────────────────────────────────────────

zid_zepto = 100005
proj_zepto = 'Zepto Chemicals'

this_datetime = datetime.now()
number_day = this_datetime.day

month_list_6 = [(this_datetime - relativedelta(months=i)).strftime('%Y/%m') for i in range(6)]

start_year = int(month_list_6[-1].split('/')[0])
start_month = int(month_list_6[-1].split('/')[1])
end_year = int(month_list_6[0].split('/')[0])
end_month = int(month_list_6[0].split('/')[1])
last_year = int(month_list_6[1].split('/')[0])
last_month = int(month_list_6[1].split('/')[1])

# Zepto Employee data
df_emp_z = get_employee(zid_zepto)
df_emp_z = df_emp_z.rename(columns={'xemp': 'xsp'})
df_emp_z['businessId'] = np.where((df_emp_z['xdept'] != ''), 'Zepto', 'HMBR')
df_emp_z.loc[df_emp_z['xsp'].str.startswith('AD'), 'businessId'] = 'Fixit'
df_emp_z.loc[df_emp_z['xsp'].str.startswith('EC'), 'businessId'] = 'E-Commerce'
df_emp_z.loc[df_emp_z['xsp'].str.startswith('RD'), 'businessId'] = 'Other'

# Zepto customer and sales data
df_cus_z = get_cus(zid_zepto).sort_values('xcus')
df_sales_z = get_sales(zid_zepto, start_year, start_month).rename(columns={'xemp': 'xsp'}).merge(df_emp_z[['xsp', 'businessId']], on=['xsp'], how='left')
df_return_z = get_return(zid_zepto, start_year, start_month).rename(columns={'xemp': 'xsp'}).merge(df_emp_z[['xsp', 'businessId']], on=['xsp'], how='left')

df_sales_g_z = df_sales_z.groupby(['xcus', 'xyear', 'xper', 'xsp', 'businessId'])['xdtwotax'].sum().reset_index().round(2)
df_return_g_z = df_return_z.groupby(['xcus', 'xyear', 'xper', 'xsp'])['totamt'].sum().reset_index().round(2)

df_zepto_g_z = df_cus_z.merge(df_sales_g_z[['xcus', 'xyear', 'xper', 'xsp', 'businessId', 'xdtwotax']], on=['xcus'], how='left').merge(df_return_g_z[['xcus', 'xyear', 'xper', 'xsp', 'totamt']], on=['xcus', 'xyear', 'xper', 'xsp'], how='left').fillna(0)

df_zepto_g_z['Zepto'] = df_zepto_g_z['xdtwotax'] - df_zepto_g_z['totamt']
df_zepto_g_z = df_zepto_g_z.drop(columns=['xdtwotax', 'totamt'])
df_zepto_g_z['xyear'] = df_zepto_g_z['xyear'].astype(np.int64)
df_zepto_g_z['xper'] = df_zepto_g_z['xper'].astype(np.int64)
df_zepto_g_z['time_line'] = df_zepto_g_z['xyear'].astype(str) + '/' + df_zepto_g_z['xper'].astype(str)
df_zepto = pd.pivot_table(df_zepto_g_z, values='Zepto', index=['xcus', 'xsp', 'businessId', 'xshort', 'xcity'], columns=['time_line'], aggfunc=np.sum).reset_index().drop(columns=['0/0']).fillna(0)

# Group by business unit
df_1 = df_zepto[(df_zepto['businessId'] == 'Zepto') & (df_zepto['xshort'] != 'Fix it.com.bd')].reset_index()
df_1.loc[len(df_1.index), :] = df_1.sum(axis=0, numeric_only=True)
df_1.at[len(df_1.index) - 1, 'xcity'] = 'Zepto'
df_1 = df_1[df_1['xcity'] == 'Zepto']

df_2 = df_zepto[(df_zepto['businessId'] == 'HMBR') & ((df_zepto['xcus'] != 'CUS-002462'))].reset_index()
df_2.loc[len(df_2.index), :] = df_2.sum(axis=0, numeric_only=True)
df_2.at[len(df_2.index) - 1, 'xcity'] = 'HMBR'
df_2 = df_2[df_2['xcity'] == 'HMBR']

df_3 = df_zepto[df_zepto['xcus'] == 'CUS-000002'].reset_index()
df_3.loc[len(df_3.index), :] = df_3.sum(axis=0, numeric_only=True)
df_3.at[len(df_3.index) - 1, 'xcity'] = 'Fixit'
df_3 = df_3[df_3['xcity'] == 'Fixit']

df_4 = df_zepto[df_zepto['xcus'] == 'CUS-000079'].reset_index()
df_4.loc[len(df_4.index), :] = df_4.sum(axis=0, numeric_only=True)
df_4.at[len(df_4.index) - 1, 'xcity'] = 'E-Commerce'
df_4 = df_4[df_4['xcity'] == 'E-Commerce']

df_5 = df_zepto[df_zepto['xcus'] == 'CUS-000004'].reset_index()
df_5.loc[len(df_5.index), :] = df_5.sum(axis=0, numeric_only=True)
df_5.at[len(df_5.index) - 1, 'xcity'] = 'General Cus'
df_5 = df_5[df_5['xcity'] == 'General Cus']

df_6 = df_zepto[df_zepto['xcus'] == 'CUS-002546'].reset_index()
df_6.loc[len(df_6.index), :] = df_6.sum(axis=0, numeric_only=True)
df_6.at[len(df_6.index) - 1, 'xcity'] = 'Daraz'
df_6 = df_6[df_6['xcity'] == 'Daraz']

df_7 = df_zepto[df_zepto['xcus'] == 'CUS-002462'].reset_index()
df_7.loc[len(df_7.index), :] = df_7.sum(axis=0, numeric_only=True)
df_7.at[len(df_7.index) - 1, 'xcity'] = 'Rahima Enterprise'
df_7 = df_7[df_7['xcity'] == 'Rahima Enterprise']

df_f = df_1.append(df_2).append(df_3).append(df_4).append(df_5).append(df_6).append(df_7).drop(['index', 'xcus', 'xsp', 'businessId', 'xshort'], axis=1).reset_index().drop(['index'], axis=1)
df_f.loc[len(df_f.index), :] = df_f.sum(axis=0, numeric_only=True).reset_index(drop=True)
df_f.at[len(df_f.index) - 1, 'xcity'] = 'Total'

# Accounts Receivable
df_acc_z = get_acc_receivable(zid_zepto, proj_zepto, end_year, end_month).rename(columns={'xsub': 'xcus'})
df_acc_z_l = get_acc_receivable(zid_zepto, proj_zepto, last_year, last_month).rename(columns={'xsub': 'xcus'})
df_acc_z = df_acc_z.merge(df_acc_z_l[['xcus', 'ar']], on=['xcus'], how='left').rename(columns={'ar_x': month_list_6[0] + 'ar', 'ar_y': month_list_6[1] + 'ar'})

df_1ar = df_acc_z[(df_acc_z['xcus'] != 'CUS-000002') & (df_acc_z['xcus'] != 'CUS-000079') & (df_acc_z['xcus'] != 'CUS-000004') & (df_acc_z['xcus'] != 'CUS-002546') & (df_acc_z['xcus'] != 'CUS-002462')].reset_index()
df_1ar.loc[len(df_1ar.index), :] = df_1ar.sum(axis=0, numeric_only=True)
df_1ar.at[len(df_1ar.index) - 1, 'xcus'] = 'Zepto & HMBR'
df_1ar = df_1ar[df_1ar['xcus'] == 'Zepto & HMBR']

df_3ar = df_acc_z[df_acc_z['xcus'] == 'CUS-000002'].reset_index()
df_3ar.loc[len(df_3ar.index), :] = df_3ar.sum(axis=0, numeric_only=True)
df_3ar.at[len(df_3ar.index) - 1, 'xcus'] = 'Fixit'
df_3ar = df_3ar[df_3ar['xcus'] == 'Fixit']

df_4ar = df_acc_z[df_acc_z['xcus'] == 'CUS-000079'].reset_index()
df_4ar.loc[len(df_4ar.index), :] = df_4ar.sum(axis=0, numeric_only=True)
df_4ar.at[len(df_4ar.index) - 1, 'xcus'] = 'E-Commerce'
df_4ar = df_4ar[df_4ar['xcus'] == 'E-Commerce']

df_5ar = df_acc_z[df_acc_z['xcus'] == 'CUS-000004'].reset_index()
df_5ar.loc[len(df_5ar.index), :] = df_5ar.sum(axis=0, numeric_only=True)
df_5ar.at[len(df_5ar.index) - 1, 'xcus'] = 'General Cus'
df_5ar = df_5ar[df_5ar['xcus'] == 'General Cus']

df_6ar = df_acc_z[df_acc_z['xcus'] == 'CUS-002546'].reset_index()
df_6ar.loc[len(df_6ar.index), :] = df_6ar.sum(axis=0, numeric_only=True)
df_6ar.at[len(df_6ar.index) - 1, 'xcus'] = 'Daraz'
df_6ar = df_6ar[df_6ar['xcus'] == 'Daraz']

df_7ar = df_acc_z[df_acc_z['xcus'] == 'CUS-002462'].reset_index()
df_7ar.loc[len(df_7ar.index), :] = df_7ar.sum(axis=0, numeric_only=True)
df_7ar.at[len(df_7ar.index) - 1, 'xcus'] = 'Rahima Enterprise'
df_7ar = df_7ar[df_7ar['xcus'] == 'Rahima Enterprise']

df_ar = df_1ar.append(df_3ar).append(df_4ar).append(df_5ar).append(df_6ar).append(df_7ar).drop(['index', 'xshort', 'xadd2', 'xcity', 'xstate'], axis=1).reset_index().drop(['index'], axis=1)
df_ar.loc[len(df_ar.index), :] = df_ar.sum(axis=0, numeric_only=True)
df_ar.at[len(df_ar.index) - 1, 'xcus'] = 'Total'

# Accounts Payable
df_ap = get_acc_payable(zid_zepto, proj_zepto, end_year, end_month).rename(columns={'xsub': 'xsup'})
df_ap_l = get_acc_payable(zid_zepto, proj_zepto, last_year, last_month).rename(columns={'xsub': 'xsup'})
df_zp = df_ap.merge(df_ap_l[['xsup', 'ap']], on=['xsup'], how='left').rename(columns={'ap_x': month_list_6[0] + 'ap', 'ap_y': month_list_6[1] + 'ap'})

# Item Sales
df_sales_i_z = df_sales_z.groupby(['xitem', 'xyear', 'xper'])['xdtwotax'].sum().reset_index().round(2)
df_return_i_z = df_return_z.groupby(['xitem', 'xyear', 'xper'])['totamt'].sum().reset_index().round(2)

df_caitem = get_item(zid_zepto)
df_item = df_caitem.merge(df_sales_i_z[['xitem', 'xyear', 'xper', 'xdtwotax']], on='xitem', how='left').merge(df_return_i_z[['xitem', 'xyear', 'xper', 'totamt']], on=['xitem', 'xyear', 'xper'], how='left').fillna(0)
df_item['Sales'] = df_item['xdtwotax'] - df_item['totamt']
df_item = df_item.drop(columns=['xdtwotax', 'totamt'])
df_item['xyear'] = df_item['xyear'].astype(np.int64)
df_item['xper'] = df_item['xper'].astype(np.int64)
df_item['time_line'] = df_item['xyear'].astype(str) + '/' + df_item['xper'].astype(str)
df_item = pd.pivot_table(df_item, values='Sales', index=['xitem', 'xdesc'], columns=['time_line'], aggfunc=np.sum).reset_index().drop(columns=['0/0']).fillna(0)

df_8 = df_item[(df_item['xitem'] == 'FZ000023') | (df_item['xitem'] == 'FZ000024') | (df_item['xitem'] == 'FZ000179')].reset_index()
df_8.loc[len(df_8.index), :] = df_8.sum(axis=0, numeric_only=True)
df_8.at[len(df_8.index) - 1, 'xdesc'] = 'Section Item Sales'
df_8 = df_8[df_8['xdesc'] == 'Section Item Sales']

df_9 = df_item[(df_item['xitem'] != 'FZ000023') & (df_item['xitem'] != 'FZ000024') & (df_item['xitem'] != 'FZ000179')].reset_index()
df_9.loc[len(df_9.index), :] = df_9.sum(axis=0, numeric_only=True)
df_9.at[len(df_9.index) - 1, 'xdesc'] = 'Non-Section Item Sales'
df_9 = df_9[df_9['xdesc'] == 'Non-Section Item Sales']

df_i = df_8.append(df_9).drop(['index', 'xitem'], axis=1).reset_index().drop(['index'], axis=1)
df_i.loc[len(df_i.index), :] = df_i.sum(axis=0, numeric_only=True)
df_i.at[len(df_i.index) - 1, 'xdesc'] = 'Total'

# Item Purchase
df_p = get_purchase(zid_zepto, start_year, start_month)
df_p['xyear'] = df_p['xyear'].astype(np.int64)
df_p['xper'] = df_p['xper'].astype(np.int64)
df_p['time_line'] = df_p['xyear'].astype(str) + '/' + df_p['xper'].astype(str)
df_purchase = pd.pivot_table(df_p, values='purchase', index=['xitem', 'xdesc'], columns=['time_line'], aggfunc=np.sum).reset_index().fillna(0)

df_10 = df_purchase[(df_purchase['xitem'] != 'FZ000023') & (df_purchase['xitem'] != 'FZ000024') & (df_purchase['xitem'] != 'FZ000179')].reset_index()
df_10.loc[len(df_10.index), :] = df_10.sum(axis=0, numeric_only=True)
df_10.at[len(df_10.index) - 1, 'xdesc'] = 'Non-Section Item Purchase'
df_10 = df_10[df_10['xdesc'] == 'Non-Section Item Purchase']

df_11 = df_purchase[(df_purchase['xitem'] == 'FZ000023') | (df_purchase['xitem'] == 'FZ000024') | (df_purchase['xitem'] == 'FZ000179')].reset_index()
df_11.loc[len(df_11.index), :] = df_11.sum(axis=0, numeric_only=True)
df_11.at[len(df_11.index) - 1, 'xdesc'] = 'Section Item Purchase'
df_11 = df_11[df_11['xdesc'] == 'Section Item Purchase']

df_j = df_11.append(df_10).drop(['index', 'xitem'], axis=1).reset_index().drop(['index'], axis=1)
df_j.loc[len(df_j.index), :] = df_j.sum(axis=0, numeric_only=True)
df_j.at[len(df_j.index) - 1, 'xdesc'] = 'Total'

# Export to Excel with HM_25 prefix


# Export to Excel with HM_25 prefix with function
with pd.ExcelWriter('HM_25_Zepto_Overall_Details.xlsx') as writer2:
    df_zepto.to_excel(writer2, "zepto")
    df_acc_z.to_excel(writer2, "accountZepto")
    df_item.to_excel(writer2, "itemZepto")
    df_purchase.to_excel(writer2, "purchaseZepto")
    df_ap.to_excel(writer2, "zeptoPayable")

OUTPUT_FILE = 'HM_25_Zepto_Overall_Details.xlsx'

# remove timeline column from all df
df_f.columns.name = None
df_ar.columns.name = None
df_i.columns.name = None
df_j.columns.name = None
df_zp.columns.name = None


html_body_list = [
        (df_f, 'Zepto Sales'),
        (df_ar, 'Zepto Account Receivable'),
        (df_i, 'Zepto Item Sales'),
        (df_j, 'Zepto Item Purchase'),
        (df_zp, 'Zepto Payable'),
      ]
try:
    # Extract report name from filename
    report_name = os.path.splitext(os.path.basename(__file__))[0]
    print(report_name)
    recipients = get_email_recipients(report_name)
    print(f"📬 Recipients: {recipients}")
except Exception as e:
    print(f"⚠️ Failed to fetch recipients: {e}")
    recipients = ["ithmbrbd@gmail.com"]  # Fallback


subject = f"HM_25 Zepto overall details"
body_text = f"""
<p>Dear Sir,</p>
<p>Please find the attachment/HTML Embeded <strong>Improve</strong> version of the subjective report.</p>

<p>Best regards,<br>
Automated Reporting System</p>
"""
# Send email
try:
    send_mail(
        subject=subject,
        bodyText=body_text,
        attachment=[OUTPUT_FILE],
        recipient=recipients,
        html_body=html_body_list
    )
    print("✅ Email sent successfully.")
except Exception as e:
    print(f"❌ Failed to send email: {e}")
    raise

engine.dispose()
print ("✅ Database connection closed. Process Completed")