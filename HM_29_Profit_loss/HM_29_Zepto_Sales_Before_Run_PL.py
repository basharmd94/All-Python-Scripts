"""
🌟 SUBJECT : 

📅 Purpose:


🧠 Logic Flow:


🗃️ Tables Used:


📬 Recipients:


***** Note ****

"""

import os
import sys
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv
import warnings

# === Load Environment & Config ===
load_dotenv()

PROJECT_ROOT = os.path.dirname(os.getcwd())
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from mail import send_mail, get_email_recipients
from project_config import DATABASE_URL

engine = create_engine(DATABASE_URL)
warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)
pd.set_option('display.float_format', '{:.2f}'.format)



def get_item(zid):
    df = pd.read_sql("""SELECT xitem,xdesc,xstdprice,xsrate 
                        FROM caitem 
                        WHERE zid = {} 
                        AND xitem LIKE '{}'""".format(zid,'FZ%%'),con=engine)
    return df

def get_sales(zid,year):
    df = pd.read_sql("""SELECT DISTINCT(imtrn.xitem), imtrn.xyear, SUM(imtrn.xqty) as qty, SUM(imtrn.xval) as cost, AVG(opddt.xrate) as rate , SUM(opddt.xdtwotax) as totamt
                        FROM imtrn
                        JOIN opddt
                        ON imtrn.xdocnum = opddt.xdornum
                        AND imtrn.xitem = opddt.xitem
                        AND imtrn.ximtrnnum = opddt.ximtrnnum
                        WHERE imtrn.zid = %s
                        AND opddt.zid = %s
                        AND imtrn.xyear >= %s
                        AND imtrn.xdoctype = '%s'
                        GROUP BY imtrn.xitem, imtrn.xyear"""%(zid,zid,year,'DO--'),con=engine)
    return df

def get_return(zid,year):
    df = pd.read_sql("""SELECT DISTINCT(imtrn.xitem), imtrn.xyear, SUM(imtrn.xqty) as rqty
                        FROM imtrn 
                        WHERE imtrn.zid = %s
                        AND imtrn.xyear >= %s
                        AND imtrn.xdoctype = '%s'
                        GROUP BY imtrn.xitem, imtrn.xyear"""%(zid,year,'SR--'),con=engine)
    return df

def customer_count(zid,year):
    df = pd.read_sql("""SELECT imtrn.xyear,COUNT(DISTINCT(imtrn.xcus))
                        FROM imtrn
                        WHERE imtrn.zid = %s
                        AND imtrn.xyear >= %s
                        AND imtrn.xdoctype = '%s'
                        GROUP BY imtrn.xyear"""%(zid,year,'DO--'),con=engine)
    return df

def day_count(zid,year):
    df = pd.read_sql("""SELECT imtrn.xyear,COUNT(DISTINCT(imtrn.xdate))
                        FROM imtrn
                        JOIN opddt
                        ON imtrn.xdocnum = opddt.xdornum
                        AND imtrn.xitem = opddt.xitem
                        AND imtrn.ximtrnnum = opddt.ximtrnnum
                        WHERE imtrn.zid = %s
                        AND opddt.zid = %s
                        AND imtrn.xyear >= %s
                        AND imtrn.xdoctype = '%s'
                        GROUP BY imtrn.xyear"""%(zid,zid,year,'DO--'),con=engine)
    return df



zepto_zid = os.getenv('ZID_ZEPTO_CHEMICALS')
year = 2018
df_i = get_item(zepto_zid)
df_s = get_sales(zepto_zid,year)
df_r = get_return(zepto_zid,year)
df_c = customer_count(zepto_zid,year)
df_y = day_count(zepto_zid,year)


df_master = df_i.merge(df_s[['xitem','xyear','qty','cost','rate','totamt']],on=['xitem'],how='left').merge(df_r[['xitem','xyear','rqty']],on=['xitem','xyear'],how='left').fillna(0)
df_master['eff_sale_qty'] = df_master['qty'] - df_master['rqty']
df_master['eff_sale_amt'] = (df_master['totamt']/df_master['qty'])*df_master['eff_sale_qty']
df_master['unit_cost'] = df_master['cost']/df_master['qty']
df_master['eff_sale_rt'] = df_master['eff_sale_amt']/df_master['eff_sale_qty']
df_master['total_cost'] = df_master['unit_cost']*df_master['eff_sale_qty']
df_master['GP'] = df_master['eff_sale_amt'] - df_master['total_cost']

conditions = [
    (df_master['eff_sale_rt'] <= 50),
    (df_master['eff_sale_rt'] > 50) & (df_master['eff_sale_rt'] <= 100),
    (df_master['eff_sale_rt'] > 100) & (df_master['eff_sale_rt'] <= 200),
    (df_master['eff_sale_rt'] > 200) & (df_master['eff_sale_rt'] <= 400),
    (df_master['eff_sale_rt'] > 400) & (df_master['eff_sale_rt'] <= 700),
    (df_master['eff_sale_rt'] > 700) & (df_master['eff_sale_rt'] <= 1500),
    (df_master['eff_sale_rt'] > 1500) & (df_master['eff_sale_rt'] <= 3000),
    (df_master['eff_sale_rt'] > 3000)]
choices = [50,100,200,400,700,1500,3000,3001]
df_master['range'] = np.select(conditions,choices, default=0)


df_rev = df_master[df_master['range']!=0].pivot_table(['eff_sale_amt'],index='range',columns=['xyear'],aggfunc='sum').round(1).fillna(0)
df_cost = df_master[df_master['range']!=0].pivot_table(['total_cost'],index='range',columns=['xyear'],aggfunc='sum').round(1).fillna(0)
df_qty = df_master[df_master['range']!=0].pivot_table(['eff_sale_qty'],index='range',columns=['xyear'],aggfunc='sum').round(1).fillna(0)
df_gp = df_master[df_master['range']!=0].pivot_table(['GP'],index='range',columns=['xyear'],aggfunc='sum').round(1).fillna(0)


# in the email attach excel reports for df_master, df_rev, df_cost, df_qty, df_gp , df_c , and df_y
# Bashar also put df_c and df_y in the excel sheet. do this ASAP. 

# Define your DataFrames: df_master, df_rev, df_cost, df_qty, df_gp, df_c, df_y
dataframes = {
    'Master': df_master,
    'Revenue': df_rev,
    'Cost': df_cost,
    'Quantity': df_qty,
    'GP': df_gp,
    'C': df_c,
    'Y': df_y
}

# Create an Excel writer object
writer = pd.ExcelWriter('zepto_sales_p.xlsx', engine='openpyxl')

# Iterate over the dictionary and write each DataFrame to a separate sheet
for sheet_name, dataframe in dataframes.items():
    dataframe.to_excel(writer, sheet_name=sheet_name, index=True)

# Save the Excel file
writer.save()

# === Email ===

try:
    # Extract report name from filename
    report_name = os.path.splitext(os.path.basename(__file__))[0]
    recipients = get_email_recipients(report_name)
    print(f"📬 Recipients: {recipients}")
except Exception as e:
    print(f"⚠️ Failed to fetch recipients: {e}")
    recipients = ["ithmbrbd@gmail.com"]  # Fallback


body_text = """
    Dear Sir,
    Please find the attached Excel/embeed HTML containing the subjective information

    Best Regards,
    Automated Reporting System
"""

send_mail(
    subject="HM_29_1: Zepto Sales before run profit and Loss",
    bodyText=body_text,
    attachment= ['zepto_sales_p.xlsx'],
    recipient=recipients

)
print("📧 Email sent successfully")

engine.dispose()
print("✅ Process completed")








