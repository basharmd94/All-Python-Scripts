
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
from datetime import datetime

def get_cus(zid):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    df = pd.read_sql("""SELECT cacus.xcus,cacus.xshort,cacus.xadd2, cacus.xcity,cacus.xstate FROM cacus WHERE zid = '%s'"""%(zid),con=engine)
    return df

def get_item(zid):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    df = pd.read_sql("""SELECT caitem.xitem,caitem.xdesc FROM caitem WHERE zid = '%s' AND xgitem = 'Industrial & Household'"""%(zid),con=engine)
    return df
## last change xlineamt to xdtwotax
def get_sales(zid,year,month):
    date = str(year) + '-' +str(month) + '-' + '01'
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    df = pd.read_sql("""SELECT imtrn.xcus,imtrn.xitem, imtrn.xyear, imtrn.xper, imtrn.xdate , imtrn.xqty,\n
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
                    AND imtrn.xdoctype = '%s'"""%(zid,zid,zid,date,'DO--'),con=engine)
    return df

def get_return(zid,year,month):
    date = str(year) + '-' +str(month) + '-' + '01'
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
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
                        AND imtrn.xdoctype = '%s'"""%(zid,zid,zid,date,'SR--'),con=engine)
    return df

def get_acc_receivable(zid, proj, year, month):
    year_month = str(year) + str(month)
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
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
                        GROUP BY gldetail.xsub,cacus.xshort,cacus.xadd2,cacus.xcity,cacus.xstate"""%(zid,zid,zid,proj,'OB--%%',year_month),con=engine)
    return df


def get_acc_payable(zid, proj, year, month):
    year_month = str(year) + str(month)
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
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
                        GROUP BY gldetail.xsub,casup.xshort"""%(zid,zid,zid,proj,'OB--%%',year_month),con=engine)
    return df

def get_employee(zid):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    df = pd.read_sql("""SELECT xemp,xname,xdept,xdesig,xstatusemp FROM prmst WHERE zid = '%s'"""%(zid),con=engine)
#     df = df[(df['xdept']=='Sales & Marketing')|(df['xdept']=='Marketing')|(df['xdept']=='Sales')]
#     df = df[df['xstatusemp']=='A-Active']
    return df
#find the different employee ID between zepto and hmbr using prmst of Both

def get_purchase(zid, year, month):
    date = str(year) + '-' +str(month) + '-' + '01'
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    df = pd.read_sql("""SELECT imtrn.zid, imtrn.xitem, imtrn.xyear, imtrn.xper, caitem.xdesc,caitem.xgitem, SUM(imtrn.xval) AS Purchase\n
                                    FROM imtrn\n
                                    JOIN caitem\n
                                    ON imtrn.xitem = caitem.xitem\n
                                    WHERE imtrn.zid = %s\n
                                    AND caitem.zid = %s\n
                                    AND imtrn.xdocnum LIKE '%s'\n
                                    AND imtrn.xdate > '%s'\n
                                    GROUP BY imtrn.zid, imtrn.xitem, imtrn.xyear, imtrn.xper, caitem.xdesc, caitem.xgitem, caitem.xstdprice"""%(zid,zid,'GRN-%%', date), con = engine)
    return df


zid_zepto = 100005
proj_zepto = 'Zepto Chemicals'

this_datetime = datetime.now()
number_day = this_datetime.day

month_list_6 = [(this_datetime - relativedelta(months=i)).strftime('%Y/%m') for i in range(6)]
# month_list_24 = [(this_datetime - relativedelta(months=i)).strftime('%Y/%m') for i in range(24)]

start_year = int(month_list_6[-1].split('/')[0])
start_month = int(month_list_6[-1].split('/')[1])
end_year = int(month_list_6[0].split('/')[0])
end_month = int(month_list_6[0].split('/')[1])
last_year =  int(month_list_6[1].split('/')[0])
last_month = int(month_list_6[1].split('/')[1])


#Zepto Employee data
df_emp_z = get_employee(zid_zepto)
df_emp_z = df_emp_z.rename(columns={'xemp':'xsp'})
df_emp_z['businessId'] = np.where((df_emp_z['xdept']!= ''), 'Zepto', 'HMBR')
df_emp_z.loc[df_emp_z['xsp'].str.startswith('AD'),'businessId'] = 'Fixit'
df_emp_z.loc[df_emp_z['xsp'].str.startswith('EC'),'businessId'] = 'E-Commerce'
df_emp_z.loc[df_emp_z['xsp'].str.startswith('RD'),'businessId'] = 'Other'


# #Zepto
df_cus_z = get_cus(zid_zepto).sort_values('xcus')
df_sales_z = get_sales(zid_zepto,start_year,start_month).rename(columns={'xemp':'xsp'}).merge(df_emp_z[['xsp','businessId']],on=['xsp'],how='left')
df_return_z = get_return(zid_zepto,start_year,start_month).rename(columns={'xemp':'xsp'}).merge(df_emp_z[['xsp','businessId']],on=['xsp'],how='left')
# # #can use this later as well in the zepto section
# # df_acc_z = get_acc_receivable(zid_zepto,proj_zepto).rename(columns={'xsub':'xcus'})

# # #final for zepto(all) customer wise which can be converted to 
df_sales_g_z = df_sales_z.groupby(['xcus','xyear','xper','xsp','businessId'])['xdtwotax'].sum().reset_index().round(2)
df_return_g_z = df_return_z.groupby(['xcus','xyear','xper','xsp'])['totamt'].sum().reset_index().round(2)

# #final for all
df_zepto_g_z = df_cus_z.merge(df_sales_g_z[['xcus','xyear','xper','xsp','businessId','xdtwotax']],on=['xcus'],how='left').merge(df_return_g_z[['xcus','xyear','xper','xsp','totamt']],on=['xcus','xyear','xper','xsp'],how='left').fillna(0)

#final for HMBR
# df_zepto_g_zh = df_zepto_g_z[df_zepto_g_z['businessId']=='HMBR']

df_zepto_g_z['Zepto'] = df_zepto_g_z['xdtwotax'] - df_zepto_g_z['totamt']
df_zepto_g_z = df_zepto_g_z.drop(columns=['xdtwotax','totamt'])
df_zepto_g_z['xyear'] = df_zepto_g_z['xyear'].astype(np.int64)
df_zepto_g_z['xper'] = df_zepto_g_z['xper'].astype(np.int64)
df_zepto_g_z['time_line'] = df_zepto_g_z['xyear'].astype(str)+'/'+df_zepto_g_z['xper'].astype(str)
df_zepto = pd.pivot_table(df_zepto_g_z,values='Zepto', index=['xcus','xsp','businessId','xshort','xcity'],columns=['time_line'],aggfunc=np.sum).reset_index().drop(columns=['0/0']).fillna(0)
# df_zepto_salesman = pd.pivot_table(df_zepto_g_z,values='Zepto', index=['xsp','businessId'],columns=['time_line'],aggfunc=np.sum).reset_index().drop(columns=['0/0']).fillna(0)


df_1 = df_zepto[(df_zepto['businessId'] == 'Zepto') & (df_zepto['xshort'] != 'Fix it.com.bd')].reset_index()
df_1.loc[len(df_1.index),:] = df_1.sum(axis=0,numeric_only=True)
df_1.at[len(df_1.index)-1,'xcity'] ='Zepto'
df_1 = df_1[df_1['xcity']=='Zepto']

df_2 = df_zepto[(df_zepto['businessId'] == 'HMBR') & ((df_zepto['xcus'] != 'CUS-002462'))].reset_index()
df_2.loc[len(df_2.index),:] = df_2.sum(axis=0,numeric_only=True)
df_2.at[len(df_2.index)-1,'xcity'] ='HMBR'
df_2 = df_2[df_2['xcity']=='HMBR']

df_3 = df_zepto[df_zepto['xcus'] == 'CUS-000002'].reset_index()
df_3.loc[len(df_3.index),:] = df_3.sum(axis=0,numeric_only=True)
df_3.at[len(df_3.index)-1,'xcity'] ='Fixit'
df_3 = df_3[df_3['xcity']=='Fixit']

df_4 = df_zepto[df_zepto['xcus'] == 'CUS-000079'].reset_index()
df_4.loc[len(df_4.index),:] = df_4.sum(axis=0,numeric_only=True)
df_4.at[len(df_4.index)-1,'xcity'] ='E-Commerce'
df_4 = df_4[df_4['xcity']=='E-Commerce']

df_5 = df_zepto[df_zepto['xcus'] == 'CUS-000004'].reset_index()
df_5.loc[len(df_5.index),:] = df_5.sum(axis=0,numeric_only=True)
df_5.at[len(df_5.index)-1,'xcity'] ='General Cus'
df_5 = df_5[df_5['xcity']=='General Cus']

df_6 = df_zepto[df_zepto['xcus'] == 'CUS-002546'].reset_index()
df_6.loc[len(df_6.index),:] = df_6.sum(axis=0,numeric_only=True)
df_6.at[len(df_6.index)-1,'xcity'] ='Daraz'
df_6 = df_6[df_6['xcity']=='Daraz']

df_7 = df_zepto[df_zepto['xcus'] == 'CUS-002462'].reset_index()
df_7.loc[len(df_7.index),:] = df_7.sum(axis=0,numeric_only=True)
df_7.at[len(df_7.index)-1,'xcity'] ='Rahima Enterprise'
df_7 = df_7[df_7['xcity']=='Rahima Enterprise']

df_f = df_1.append(df_2).append(df_3).append(df_4).append(df_5).append(df_6).append(df_7).drop(['index','xcus','xsp','businessId','xshort'], axis=1).reset_index().drop(['index'],axis=1)
df_f.loc[len(df_f.index),:] = df_f.sum(axis=0,numeric_only=True)
df_f.at[len(df_f.index)-1,'xcity'] ='Total'

df_acc_z = get_acc_receivable(zid_zepto,proj_zepto,end_year,end_month).rename(columns={'xsub':'xcus'})
df_acc_z_l = get_acc_receivable(zid_zepto,proj_zepto,last_year,last_month).rename(columns={'xsub':'xcus'})
df_acc_z = df_acc_z.merge(df_acc_z_l[['xcus','ar']],on=['xcus'],how='left').rename(columns={'ar_x':month_list_6[0]+'ar','ar_y':month_list_6[1]+'ar'})
# df_zepto_summary = df_acc_z.groupby(['xcus','xshort'])[month_list_6[0]+'ar',month_list_6[1]+'ar'].sum().reset_index().round(2)

# df_zepto = df_zepto.merge(df_acc_z[['xcus',month_list_6[0]+'ar',month_list_6[1]+'ar']],on='xcus',how='left').fillna(0)


df_1ar = df_acc_z[(df_acc_z['xcus'] != 'CUS-000002') & (df_acc_z['xcus'] != 'CUS-000079') & (df_acc_z['xcus'] != 'CUS-000004') & (df_acc_z['xcus'] != 'CUS-002546') & (df_acc_z['xcus'] != 'CUS-002462')].reset_index()
df_1ar.loc[len(df_1ar.index),:] = df_1ar.sum(axis=0,numeric_only=True)
df_1ar.at[len(df_1ar.index)-1,'xcus'] ='Zepto & HMBR'
df_1ar = df_1ar[df_1ar['xcus']=='Zepto & HMBR']

df_3ar = df_acc_z[df_acc_z['xcus'] == 'CUS-000002'].reset_index()
df_3ar.loc[len(df_3ar.index),:] = df_3ar.sum(axis=0,numeric_only=True)
df_3ar.at[len(df_3ar.index)-1,'xcus'] ='Fixit'
df_3ar = df_3ar[df_3ar['xcus']=='Fixit']

df_4ar = df_acc_z[df_acc_z['xcus'] == 'CUS-000079'].reset_index()
df_4ar.loc[len(df_4ar.index),:] = df_4ar.sum(axis=0,numeric_only=True)
df_4ar.at[len(df_4ar.index)-1,'xcus'] ='E-Commerce'
df_4ar = df_4ar[df_4ar['xcus']=='E-Commerce']

df_5ar = df_acc_z[df_acc_z['xcus'] == 'CUS-000004'].reset_index()
df_5ar.loc[len(df_5ar.index),:] = df_5ar.sum(axis=0,numeric_only=True)
df_5ar.at[len(df_5ar.index)-1,'xcus'] ='General Cus'
df_5ar = df_5ar[df_5ar['xcus']=='General Cus']

df_6ar = df_acc_z[df_acc_z['xcus'] == 'CUS-002546'].reset_index()
df_6ar.loc[len(df_6ar.index),:] = df_6ar.sum(axis=0,numeric_only=True)
df_6ar.at[len(df_6ar.index)-1,'xcus'] ='Daraz'
df_6ar = df_6ar[df_6ar['xcus']=='Daraz']

df_7ar = df_acc_z[df_acc_z['xcus'] == 'CUS-002462'].reset_index()
df_7ar.loc[len(df_7ar.index),:] = df_7ar.sum(axis=0,numeric_only=True)
df_7ar.at[len(df_7ar.index)-1,'xcus'] ='Rahima Enterprise'
df_7ar = df_7ar[df_7ar['xcus']=='Rahima Enterprise']

df_ar = df_1ar.append(df_3ar).append(df_4ar).append(df_5ar).append(df_6ar).append(df_7ar).drop(['index','xshort','xadd2','xcity','xstate'], axis=1).reset_index().drop(['index'],axis=1)
df_ar.loc[len(df_ar.index),:] = df_ar.sum(axis=0,numeric_only=True)
df_ar.at[len(df_ar.index)-1,'xcus'] ='Total'

df_ap = get_acc_payable(zid_zepto,proj_zepto,end_year,end_month).rename(columns={'xsub':'xsup'})
df_ap_l = get_acc_payable(zid_zepto,proj_zepto,last_year,last_month).rename(columns={'xsub':'xsup'})
df_zp = df_ap.merge(df_ap_l[['xsup','ap']],on=['xsup'],how='left').rename(columns={'ap_x':month_list_6[0]+'ap','ap_y':month_list_6[1]+'ap'})


df_sales_i_z = df_sales_z.groupby(['xitem','xyear','xper'])['xdtwotax'].sum().reset_index().round(2)
df_return_i_z = df_return_z.groupby(['xitem','xyear','xper'])['totamt'].sum().reset_index().round(2)
# df_zepto_g_z = df_cus_z.merge(df_sales_g_z[['xcus','xyear','xper','xsp','businessId','xdtwotax']],on=['xcus'],how='left').merge(df_return_g_z[['xcus','xyear','xper','xsp','totamt']],on=['xcus','xyear','xper','xsp'],how='left').fillna(0)

df_caitem = get_item(zid_zepto)
df_item = df_caitem.merge(df_sales_i_z[['xitem','xyear','xper','xdtwotax']],on='xitem',how='left').merge(df_return_i_z[['xitem','xyear','xper','totamt']],on=['xitem','xyear','xper'],how='left').fillna(0)
df_item['Sales'] = df_item['xdtwotax'] - df_item['totamt']

df_item = df_item.drop(columns=['xdtwotax','totamt'])
df_item['xyear'] = df_item['xyear'].astype(np.int64)
df_item['xper'] = df_item['xper'].astype(np.int64)
df_item['time_line'] = df_item['xyear'].astype(str)+'/'+df_item['xper'].astype(str)
df_item = pd.pivot_table(df_item,values='Sales', index=['xitem','xdesc'],columns=['time_line'],aggfunc=np.sum).reset_index().drop(columns=['0/0']).fillna(0)

df_8 = df_item[(df_item['xitem'] == 'FZ000023') | (df_item['xitem'] == 'FZ000024') | (df_item['xitem'] == 'FZ000179')].reset_index()
df_8.loc[len(df_8.index),:] = df_8.sum(axis=0,numeric_only=True)
df_8.at[len(df_8.index)-1,'xdesc'] ='Section Item Sales'
df_8 = df_8[df_8['xdesc']=='Section Item Sales']

df_9 = df_item[(df_item['xitem'] != 'FZ000023')&(df_item['xitem'] != 'FZ000024') & (df_item['xitem'] != 'FZ000179')].reset_index()
df_9.loc[len(df_9.index),:] = df_9.sum(axis=0,numeric_only=True)
df_9.at[len(df_9.index)-1,'xdesc'] ='Non-Section Item Sales'
df_9 = df_9[df_9['xdesc']=='Non-Section Item Sales']

df_i = df_8.append(df_9).drop(['index','xitem'], axis=1).reset_index().drop(['index'],axis=1)
df_i.loc[len(df_i.index),:] = df_i.sum(axis=0,numeric_only=True)
df_i.at[len(df_i.index)-1,'xdesc'] ='Total'

df_p = get_purchase(zid_zepto,start_year,start_month)
df_p['xyear'] = df_p['xyear'].astype(np.int64)
df_p['xper'] = df_p['xper'].astype(np.int64)
df_p['time_line'] = df_p['xyear'].astype(str)+'/'+df_p['xper'].astype(str)
df_purchase = pd.pivot_table(df_p,values='purchase',index=['xitem','xdesc'],columns=['time_line'],aggfunc=np.sum).reset_index().fillna(0)

df_10 = df_purchase[(df_purchase['xitem'] != 'FZ000023')&(df_purchase['xitem'] != 'FZ000024') & (df_purchase['xitem'] != 'FZ000179')].reset_index()
df_10.loc[len(df_10.index),:] = df_10.sum(axis=0,numeric_only=True)
df_10.at[len(df_10.index)-1,'xdesc'] ='Non-Section Item Purchase'
df_10 = df_10[df_10['xdesc']=='Non-Section Item Purchase']

df_11 = df_purchase[(df_purchase['xitem'] == 'FZ000023') | (df_purchase['xitem'] == 'FZ000024') | (df_purchase['xitem'] == 'FZ000179')].reset_index()
df_11.loc[len(df_11.index),:] = df_11.sum(axis=0,numeric_only=True)
df_11.at[len(df_11.index)-1,'xdesc'] ='Section Item Purchase'
df_11 = df_11[df_11['xdesc']=='Section Item Purchase']

df_j = df_11.append(df_10).drop(['index','xitem'], axis=1).reset_index().drop(['index'],axis=1)
df_j.loc[len(df_j.index),:] = df_j.sum(axis=0,numeric_only=True)
df_j.at[len(df_j.index)-1,'xdesc'] ='Total'

#####


#attach as excel(same workbook but different sheet for each) 
#df_zepto, df_acc_z, df_item, df_purchase

writer2 = pd.ExcelWriter('ZeptoOverall.xlsx')

df_zepto.to_excel(writer2,"zepto")
df_acc_z.to_excel(writer2,"accountZepto")
df_item.to_excel(writer2,"itemZepto")
df_purchase.to_excel(writer2,"purchaseZepto")
df_ap.to_excel(writer2, "zeptoPayable" )

writer2.save()

# print in html (within the email)
# df_f, df_ar, df_i,df_j, df_zp
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
# df_f, df_ar, df_i,df_j, df_zp
with open("index.html",'w') as f:
    f.write(HEADER)
    f.write("<h2 style='color:red;'>Zepto Sales</h2>")
    f.write(df_f.to_html(classes='df_f'))
    f.write("<h2 style='color:red;'>Zepto Account Receivable</h2>")
    f.write(df_ar.to_html(classes='df_ar'))
    f.write("<h2 style='color:red;'>Zepto Item Sales</h2>")
    f.write(df_i.to_html(classes='df_i'))
    f.write("<h2 style='color:red;'>Zepto Item Purchase</h2>")
    f.write(df_j.to_html(classes='df_j'))
    f.write("<h2 style='color:red;'>Zepto Payable</h2>")
    f.write(df_zp.to_html(classes='df_zp'))
    f.write(FOOTER)



#mail
me = "pythonhmbr12@gmail.com"

you = ["ithmbrbd@gmail.com"]

msg = MIMEMultipart('alternative')
msg['Subject'] = "Zepto Overall Details"
msg['From'] = me
msg['To'] = ", ".join(you)

filename = "index.html"
f = open(filename)
attachment = MIMEText(f.read(),'html')
msg.attach(attachment)




part1 = MIMEBase('application', "octet-stream")
part1.set_payload(open("ZeptoOverall.xlsx", "rb").read())
encoders.encode_base64(part1)
part1.add_header('Content-Disposition', 'attachment; filename="ZeptoOverall.xlsx"')
msg.attach(part1)




username = 'pythonhmbr12'
password = 'vksikttussvnbqef'

s = smtplib.SMTP('smtp.gmail.com:587')
s.starttls()
s.login(username, password)
s.sendmail(me,you,msg.as_string())
s.quit()

