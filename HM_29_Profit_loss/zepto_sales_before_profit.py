# %%
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from datetime import date,datetime,timedelta
import psycopg2
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import openpyxl
import xlrd
from dateutil.relativedelta import relativedelta

# %% [markdown]
# 

# %%
def get_item(zid):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    df = pd.read_sql("""SELECT xitem,xdesc,xstdprice,xsrate 
                        FROM caitem 
                        WHERE zid = {} 
                        AND xitem LIKE '{}'""".format(zid,'FZ%%'),con=engine)
    return df

def get_sales(zid,year):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
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
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    df = pd.read_sql("""SELECT DISTINCT(imtrn.xitem), imtrn.xyear, SUM(imtrn.xqty) as rqty
                        FROM imtrn 
                        WHERE imtrn.zid = %s
                        AND imtrn.xyear >= %s
                        AND imtrn.xdoctype = '%s'
                        GROUP BY imtrn.xitem, imtrn.xyear"""%(zid,year,'SR--'),con=engine)
    return df

def customer_count(zid,year):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    df = pd.read_sql("""SELECT imtrn.xyear,COUNT(DISTINCT(imtrn.xcus))
                        FROM imtrn
                        WHERE imtrn.zid = %s
                        AND imtrn.xyear >= %s
                        AND imtrn.xdoctype = '%s'
                        GROUP BY imtrn.xyear"""%(zid,year,'DO--'),con=engine)
    return df

def day_count(zid,year):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
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


# %%
zepto_zid = 100005
year = 2018
df_i = get_item(zepto_zid)
df_s = get_sales(zepto_zid,year)
df_r = get_return(zepto_zid,year)
df_c = customer_count(zepto_zid,year)
df_y = day_count(zepto_zid,year)

# %%
df_master = df_i.merge(df_s[['xitem','xyear','qty','cost','rate','totamt']],on=['xitem'],how='left').merge(df_r[['xitem','xyear','rqty']],on=['xitem','xyear'],how='left').fillna(0)

# %%
# df_hmbr_g_h = df_cus.merge(df_sales_g_h[['xcus','xyear','xper','xsp','xlineamt']],on=['xcus'],how='left').merge(df_return_g_h[['xcus','xyear','xper','xsp','totamt']],on=['xcus','xyear','xper','xsp'],how='left').fillna(0

# %%
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

# %%
df_rev = df_master[df_master['range']!=0].pivot_table(['eff_sale_amt'],index='range',columns=['xyear'],aggfunc='sum').round(1).fillna(0)
df_cost = df_master[df_master['range']!=0].pivot_table(['total_cost'],index='range',columns=['xyear'],aggfunc='sum').round(1).fillna(0)
df_qty = df_master[df_master['range']!=0].pivot_table(['eff_sale_qty'],index='range',columns=['xyear'],aggfunc='sum').round(1).fillna(0)
df_gp = df_master[df_master['range']!=0].pivot_table(['GP'],index='range',columns=['xyear'],aggfunc='sum').round(1).fillna(0)

# %%
# in the email attach excel reports for df_master, df_rev, df_cost, df_qty, df_gp , df_c , and df_y
# Bashar also put df_c and df_y in the excel sheet. do this ASAP. 

# %%

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

# %%

def send_email_with_attachments(sender_email, sender_password, recipient_emails, subject, body, attachments):
    # Establish a connection to the SMTP server
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(sender_email, sender_password)

    # Create a multipart message container
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = ', '.join(recipient_emails)
    message['Subject'] = subject

    # Add the email body as plain text
    message.attach(MIMEText(body, 'plain'))

    # Attach the files
    for attachment in attachments:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(open(attachment, 'rb').read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{attachment}"')
        message.attach(part)

    # Send the email
    server.send_message(message)

    # Clean up the connection
    server.quit()

# Sender and recipient details
sender_email = 'pythonhmbr12@gmail.com'
sender_password = 'vksikttussvnbqef'
recipient_emails = ['ithmbrbd@gmail.com', ]

# Email content
subject = 'Zepto Sales before run profit and Loss'
body = 'Please find attached the files you requested.'

# Attachments
attachments = ['zepto_sales_p.xlsx']

# Send the email
send_email_with_attachments(sender_email, sender_password, recipient_emails, subject, body, attachments)

# %%



