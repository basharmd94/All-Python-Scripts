# %%
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
from email.mime.image import MIMEImage
from email import encoders
from datetime import datetime
# for visualization


# %%
# Get today's date
# today = date.today()
# # Calculate the date one year ago
# last_year_date = today - timedelta(days=365)
# # Format the last year's date as a string in 'YYYY-MM-DD' format
# formatted_date = last_year_date.strftime('%Y-%m-%d')

start_date = '2022-01-01'
end_date =  '2022-12-31'
zid = 100001
print (start_date)
# %%
# %%
# Sales Function
remain_percent = 1
print (remain_percent, '%')

engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
def get_sales(zid, start_date , end_date):
    df = pd.read_sql(f"""SELECT
    opdor.xdornum, opdor.xdate, opdor.xsp, prmst.xname, opdor.xdiv, opdor.xcus, cacus.xshort, opddt.xitem, caitem.xdesc,caitem.xabc,
    SUM(opddt.xqty) as qty,
    SUM(opddt.xdtwotax) as total_amount,
    imtrn.xval
FROM
    opdor
    INNER JOIN opddt ON opdor.xdornum = opddt.xdornum
    INNER JOIN prmst ON opdor.xsp = prmst.xemp
    INNER JOIN cacus ON opdor.xcus = cacus.xcus
    INNER JOIN caitem ON opddt.xitem = caitem.xitem
    INNER JOIN (
        SELECT
            xdocnum,
            xitem,
            SUM(xval) AS xval
        FROM
            imtrn
        WHERE
            zid = {zid}
        GROUP BY
            xdocnum,
            xitem
    ) AS imtrn ON opdor.xdornum = imtrn.xdocnum AND opddt.xitem = imtrn.xitem
WHERE
    opdor.zid = {zid}
    AND opddt.zid = {zid}
    AND prmst.zid = {zid}
    AND cacus.zid = {zid}
    AND caitem.zid = {zid}
    AND  opdor.xdate between '{start_date}' and '{end_date}'

GROUP BY
    opdor.xdornum, opdor.xdate, prmst.xname, opdor.xsp, opdor.xdiv, opdor.xcus, cacus.xshort, opddt.xitem, caitem.xdesc, caitem.xabc, imtrn.xval
ORDER BY
	opdor.xdornum""",con=engine)
    return df
    
remain_percent = 20
print (remain_percent, '%')

# %%
# ==================== get the sales and imtrn xval or cost ========================

df_sales = get_sales (100001, start_date , end_date)
df_sales.head(5)
df_sales['xdate'] = pd.to_datetime(df_sales['xdate'])
df_sales.head(4)
df_sales['Year'] = df_sales['xdate'].dt.year
df_sales['Month'] = df_sales['xdate'].dt.strftime('%B')
new_columns = ['xdornum','xdate', 'Year', 'Month', 'xsp', 'xname', 'xdiv', 'xcus', 'xshort', 'xitem', 'xdesc','xabc', 'qty', 'total_amount' ] # without xval
df_sales = df_sales.reindex(columns=new_columns)

df_sales.head(2)

# %%
df_sales = df_sales.rename(columns={
    'xdornum': 'OrderNumber', 
    'xdate': 'Date', 
    'xsp': 'SP_ID',
     'xname': 'SP_Name', 
     'xdiv': 'Area', 
     'xcus': 'CustomerID',
    'xshort': 'CustomerName',
    'xitem': 'ProductCode', 
    'xdesc': 'ProductName', 
    'qty': 'Quantity',
    'xabc' : 'ProductGroup',
    'total_amount': 'TotalSales'
})

df_sales.head(2)

# %%
df_sales['Date'] = df_sales['Date'].astype(str)

# %%
get_year = start_date.split('-')[0]
df_sales.to_excel(f"""one_year_sale{get_year}.xlsx""" , sheet_name= 'oneyear_sale')

# %%

# ==================== get the return from last years ========================
def get_return(zid, start_date, end_date):
    df = pd.read_sql(f"""SELECT opcrn.xcrnnum, opcrn.xdate, opcrn.xcus,cacus.xshort,cacus.xcity, opcrn.xemp, prmst.xname, opcdt.xitem, caitem.xdesc, caitem.xabc, sum(opcdt.xqty) as ret_qty, sum(opcdt.xlineamt) as ret_total
                            FROM opcrn
                            JOIN opcdt
                            ON opcrn.xcrnnum = opcdt.xcrnnum
                            JOIN prmst
                            ON opcrn.xemp = prmst.xemp
                            JOIN caitem
                            ON opcdt.xitem = caitem.xitem
                            JOIN cacus
                            ON opcrn.xcus = cacus.xcus
                            AND cacus.zid = {zid}
                            AND opcrn.zid = {zid}
                            AND opcdt.zid = {zid} 
                            AND prmst.zid = {zid} 
                            AND caitem.zid = {zid} 
                            AND opcrn.xdate between '{start_date}' and '{end_date}'
                            group by opcrn.xcrnnum, opcrn.xdate, opcrn.xcus,cacus.xshort ,cacus.xcity,opcrn.xemp, prmst.xname, opcdt.xitem, caitem.xdesc, caitem.xabc, opcdt.xitem """, con = engine)
    return df


# %%
# ==================== Group by Sales ========================
df_sales_groupby = df_sales.groupby(['ProductCode']).sum().reset_index()
try:
    df_sales_groupby['xval'] = df_sales_groupby['xval'].astype(int)
except:
    pass

remain_percent = 50
print (remain_percent, '%')
# %%
# ==================== Group by returns by items ========================
df_get_return = get_return(100001 , start_date, end_date)
df_get_return

# create year and month column
df_get_return['xdate'] = df_get_return['xdate'].astype(str)
df_get_return[['Year', 'Month']] = df_get_return['xdate'].str.split('-', expand=True)[[0, 1]]
df_get_return['Month'] = pd.to_datetime(df_get_return['Month'], format='%m').dt.month_name()

df_get_return.head(2)

# %%
# ==================== Merge with sales and return ========================
df_sales_n_return = pd.merge(df_sales_groupby, df_get_return, left_on='ProductCode', right_on='xitem', how = 'left')\
                        .rename(columns={
                            'xitem' : 'item_code',
                            'qty' : 'sales_qty',
                            'xval' : 'imtrn_value_cost'
                        })
df_return_group_by = df_get_return.groupby([ 'xitem']).sum().reset_index()
df_return_group_by = df_get_return.rename(columns={ 'xqty': 'ret_qty'})
df_return_group_by.head(2)

remain_percent = 70
print (remain_percent, '%')
# %%
df_sales_return = pd.merge(df_sales_groupby, df_return_group_by, left_on='ProductCode', right_on='xitem', how= 'left').fillna(0)\
                .rename(columns={
                    'qty' : 'sale_qty' ,
                    'xval' : 'imtrn_cost_val',
                    'ret_total' : 'ret_amt'
                
                }).drop(columns=['Year_x'])

# df_sales_return['xdate'] = pd.to_datetime(df_sales_return['xdate'])
# df_sales_return['month'] = df_sales_return['xdate'].dt.month
# df_sales_return['year'] = df_sales_return['xdate'].dt.year



# df_sales_return.to_excel('text_2.xlsx')
df_sales_return.head(2)

# %%
for index, column in enumerate(df_sales_return.columns):
    print ("'" + column + "': '', ")

# %%


df_sales_return = df_sales_return.rename(columns={
    'Quantity': 'SalesQty', 
    'TotalSales': 'TotalSalesAmt', 
    'xcrnnum': 'ReturnVoucherNumber', 
    'xdate': 'Date', 
    'xcus': 'CustomerID', 
    'xshort': 'CustomerName', 
    'xcity': 'Area', 
    'xemp': 'SP_ID', 
    'xname': 'SP_Name', 
    'xitem': 'ProductCode', 
    'xdesc': 'ProductName', 
    'xgitem': 'ItemGroup', 
    'ret_qty': 'ReturnQty', 
    'ret_amt': 'TotalReturn', 
    'Year_y': 'Year',
    'xabc' : 'ProductGroup'
})
df_sales_return.head(2)

# %%
for index, column in enumerate(df_sales_return.columns):
    print (f"{index} " + column + "': '', ")

# %%
# reindex
df_sales_return = df_sales_return.iloc[:,[3,4,15,16,8,9,5,6,7,10,11,12,1,2,13,14]]

df_sales_return.head(2)

# %%
df_sales_return = df_sales_return.replace(0, np.nan)


df_sales_return = df_sales_return.dropna()
df_sales_return.head(2)
remain_percent = 80
print (remain_percent, '%')
# %%
from openpyxl import load_workbook

# Read the existing Excel file

file_path = f"""one_year_sale{get_year}.xlsx"""
book = load_workbook(file_path)

# # Create a new sheet for the dataframe
# new_sheet_name = "sales_return_new"
# writer = pd.ExcelWriter(file_path, engine="openpyxl")
# writer.book = book
# writer.sheets = dict((ws.title, ws) for ws in book.worksheets)

# # Write the dataframe to the new sheet
# df_sales_return.to_excel(writer, sheet_name=new_sheet_name, index=False)

# # Save the changes
# writer.save()
# writer.close()

remain_percent = 90
print (remain_percent, '%')

import traceback

try:
    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        writer.book = book
        writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
        df_sales_return.to_excel(writer, sheet_name='return', index=False)
except Exception as e:
    print(f"An error occurred: {str(e)}")
    traceback.print_exc()  # Print traceback for detailed error information

remain_percent = 100
print (remain_percent, '%')

print ("Task finished")







# %%

# sender_email = 'pythonhmbr12@gmail.com'
# receiver_emails = ['ithmbrbd@gmail.com', 'analysthmbr@gmail.com']
# subject = 'H_64.Customer, Salesman, and areawise Product Last One YearSales without xval'
# attachment_path = 'one_year_sale.xlsx'


# # body = f'Hello,\n\nPlease find the attached {subject} file.'
# body = f'Hello,\n\nPlease find the attached File'
# # Create the email message
# message = MIMEMultipart()
# message['From'] = sender_email
# message['To'] = ', '.join(receiver_emails)
# message['Subject'] = subject

# # Add the body to the email
# message.attach(MIMEText(body, 'plain'))

# # Attach the file
# with open(attachment_path, 'rb') as attachment:
#     part = MIMEBase('application', 'octet-stream')
#     part.set_payload(attachment.read())
#     encoders.encode_base64(part)
#     part.add_header('Content-Disposition', f'attachment; filename={attachment_path}')
#     message.attach(part)

# # Connect to the SMTP server and send the email
# smtp_server = 'smtp.gmail.com'
# smtp_port = 587
# smtp_username = 'pythonhmbr12@gmail.com'
# smtp_password = 'vksikttussvnbqef'

# with smtplib.SMTP(smtp_server, smtp_port) as server:
#     server.starttls()
#     server.login(smtp_username, smtp_password)
#     server.send_message(message)
#     server.quit()

# print('Email sent successfully!')


# # %%



