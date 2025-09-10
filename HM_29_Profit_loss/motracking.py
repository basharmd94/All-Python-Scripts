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

def get_mo_details(zid,date):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/hmbr')
    df = pd.read_sql("""SELECT moord.xitem, caitem.xdesc,caitem.xstdprice, moord.zid, moodt.xmoord, moord.xdatemo ,SUM((moodt.xqty*moodt.xrate)/moord.xqtyprd) as unit
                        FROM moord
                        JOIN
                        moodt
                        ON moord.xmoord = moodt.xmoord
                        JOIN
                        caitem
                        ON moord.xitem = caitem.xitem
                        WHERE moord.zid = '%s'
                        AND moodt.zid = '%s'
                        AND caitem.zid = '%s'
                        AND moord.xdatemo >= '%s'
                        GROUP BY moord.xitem,caitem.xdesc,caitem.xstdprice,moord.zid, moodt.xmoord, moord.xdatemo
                        ORDER BY caitem.xdesc ASC, moord.xdatemo """%(zid,zid,zid,date),con = engine)
    df = df[['zid','xitem','xdesc','xmoord','xdatemo','unit','xstdprice']]
    df['unit'] = df['unit'].round(2)
    return df


zid_list_hmbr = [100002,100003,100004,100005,100007,100008,100009]

starting_date = datetime.now().today().date() - timedelta(30)

starting_date = starting_date.strftime("%Y-%m-%d")

main_dict_mo = {}
for i in zid_list_hmbr:
    df = get_mo_details(i,starting_date)
    main_dict_mo[i] = df

##dataframe from dictionary

chemical_mo = main_data_mo[100002]
thread_mo = main_data_mo[100003]
plastic_mo= main_data_mo[100004]
zepto_mo = main_data_mo[100005]
paint_mo = main_data_mo[100007]
steel_mo = main_data_mo[100008]
packaging_mo = main_data_mo[100009]


##dictionary to excel

with pd.ExcelWriter('modetail.xlsx') as writer:  
    chemical_mo.to_excel(writer, sheet_name='100002')
    thread_mo.to_excel(writer, sheet_name='100003')
    plastic_mo.to_excel(writer, sheet_name='100004')
    zepto_mo.to_excel(writer, sheet_name='100005')
    paint_mo.to_excel(writer, sheet_name='100007')
    steel_mo.to_excel(writer, sheet_name='100008')
    packaging_mo.to_excel(writer, sheet_name='100009')

###Email    
me = "pythonhmbr12@gmail.com"
you = ["asaddat87@gmail.com","admhmbr@gmail.com"]

msg = MIMEMultipart('alternative')
msg['Subject'] = "MO HMBR"
msg['From'] = me
msg['To'] = ", ".join(you)

HEADER = '''
<html>
    <head>
    </head>
    <body>
'''
FOOTER = '''
    </body>
</html>
'''
with open('hello.html','w') as f:
    f.write(HEADER)
    f.write('Chemical Details')
    f.write(chemical_mo.to_html(classes='df_summery2'))
    f.write('Thread Tape Details')
    f.write(thread_mo.to_html(classes='df_summery3'))
    f.write('Plastic Details')
    f.write(plastic_mo.to_html(classes='df_summery4'))
    f.write('Zepto Details')
    f.write(zepto_mo.to_html(classes='df_summery5'))
    f.write('Paint Roller Details')
    f.write(paint_mo.to_html(classes='df_summery7'))
    f.write('Steel Scrubber Details')
    f.write(steel_mo.to_html(classes='df_summery8'))
    f.write('Packaging Details')
    f.write(packaging_mo.to_html(classes='df_summery9'))
    f.write(FOOTER)

filename = "hello.html"
f = open(filename)
attachment = MIMEText(f.read(),'html')
msg.attach(attachment)

part1 = MIMEBase('application', "octet-stream")
part1.set_payload(open("modetail.xlsx", "rb").read())
encoders.encode_base64(part1)
part1.add_header('Content-Disposition', 'attachment; filename="modetail.xlsx"')
msg.attach(part1)

username = 'pythonhmbr12@gmail.com'
password = 'vksikttussvnbqef'

s = smtplib.SMTP('smtp.gmail.com:587')
s.starttls()
s.login(username, password)
s.sendmail(me,you,msg.as_string())
s.quit()


