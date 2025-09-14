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

def get_sales_COGS(zid,start_date,end_date):
    engine = create_engine('postgresql://postgres:postgres@localhost/da')
    df = pd.read_sql("""SELECT caitem.zid,caitem.xitem,caitem.xdesc,caitem.xgitem, (imtrn.xqty*imtrn.xsign) as qty, (imtrn.xval*imtrn.xsign) as totalvalue, opddt.xqty,opddt.xrate,opddt.xlineamt,(opddt.xdtwotax-opddt.xdtdisc) as xdtwotax
                        FROM caitem
                        JOIN imtrn
                        ON caitem.xitem = imtrn.xitem
                        JOIN opddt
                        ON (imtrn.xdocnum = opddt.xdornum AND imtrn.xitem = opddt.xitem AND imtrn.xdocrow = opddt.xrow)
                        JOIN opdor
                        ON imtrn.xdocnum = opdor.xdornum
                        WHERE caitem.zid = '%s'
                        AND imtrn.zid = '%s'
                        AND opddt.zid = '%s'
                        AND opdor.zid = '%s'
                        AND imtrn.xdoctype = '%s'
                        AND imtrn.xdate >= '%s'
                        AND imtrn.xdate <= '%s'"""%(zid,zid,zid,zid,'DO--',start_date,end_date),con=engine)
    return df

def get_return(zid,start_date,end_date):
    engine = create_engine('postgresql://postgres:postgres@localhost/da')
    df = pd.read_sql("""SELECT imtrn.xitem, imtrn.xqty, (imtrn.xval*imtrn.xsign) as returnvalue, opcdt.xrate, (opcdt.xrate*imtrn.xqty) as totamt
                        FROM imtrn 
                        JOIN opcdt
                        ON imtrn.xdocnum = opcdt.xcrnnum
                        AND imtrn.xitem = opcdt.xitem
                        JOIN opcrn
                        ON imtrn.xdocnum = opcrn.xcrnnum
                        WHERE imtrn.zid = '%s'
                        AND opcdt.zid = '%s'
                        AND opcrn.zid = '%s'
                        AND imtrn.xdoctype = '%s'
                        AND imtrn.xdate >= '%s'
                        AND imtrn.xdate <= '%s'"""%(zid,zid,zid,'SR--',start_date,end_date),con=engine)
    return df

def get_gl_details(zid,COGS,start_date,end_date):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    df = pd.read_sql("""SELECT glmst.xacctype, SUM(gldetail.xprime)
                        FROM glmst
                        JOIN gldetail
                        ON glmst.xacc = gldetail.xacc
                        JOIN glheader
                        ON gldetail.xvoucher = glheader.xvoucher
                        WHERE glmst.zid = '%s'
                        AND gldetail.zid = '%s'
                        AND glheader.zid = '%s'
                        AND (glmst.xacctype = 'Income' OR glmst.xacctype = 'Expenditure')
                        AND glmst.xacc != '%s'
                        AND glheader.xdate >= '%s'
                        AND glheader.xdate <= '%s'
                        GROUP BY glmst.xacctype"""%(zid,zid,zid,COGS,start_date,end_date),con = engine)
    return df

def get_gl_details_zepto(zid,COGS,MRP,start_date,end_date):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    df = pd.read_sql("""SELECT glmst.xacctype, SUM(gldetail.xprime)
                        FROM glmst
                        JOIN gldetail
                        ON glmst.xacc = gldetail.xacc
                        JOIN glheader
                        ON gldetail.xvoucher = glheader.xvoucher
                        WHERE glmst.zid = '%s'
                        AND gldetail.zid = '%s'
                        AND glheader.zid = '%s'
                        AND (glmst.xacctype = 'Income' OR glmst.xacctype = 'Expenditure')
                        AND glmst.xacc != '%s'
                        AND glmst.xacc != '%s'
                        AND glheader.xdate >= '%s'
                        AND glheader.xdate <= '%s'
                        GROUP BY glmst.xacctype"""%(zid,zid,zid,COGS,MRP,start_date,end_date),con = engine)
    return df

def get_gl_details_project(zid,project,start_date,end_date,COGS):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    df = pd.read_sql("""SELECT glmst.xacctype, SUM(gldetail.xprime)
                        FROM glmst
                        JOIN gldetail
                        ON glmst.xacc = gldetail.xacc
                        JOIN glheader
                        ON gldetail.xvoucher = glheader.xvoucher
                        WHERE glmst.zid = '%s'
                        AND gldetail.zid = '%s'
                        AND glheader.zid = '%s'
                        AND gldetail.xproj = '%s'
                        AND (glmst.xacctype = 'Income' OR glmst.xacctype = 'Expenditure')
                        AND glmst.xacc != '%s'
                        AND glheader.xdate >= '%s'
                        AND glheader.xdate <= '%s'
                        GROUP BY  glmst.xacctype"""%(zid,zid,zid,project,COGS,start_date,end_date),con = engine)
    return df

zid_list = [100002,100003,100005,100006,100007,100008,100009]
# zid_list_fixit = [100000,100001,100002,100003]
zid_trade = 100001
zid_plastic = 100004
zid_karigor = 100000

project_trade = 'GULSHAN TRADING'

project_plastic = 'Gulshan Plastic'

project_karigor = 'Karigor Ltd.'

COGS_zepto = '04010020'

COGS = '04010020'

end_date = (datetime.now() - timedelta(days = 2)).strftime('%Y-%m-%d')
start_date = (datetime.now() - timedelta(days = 33)).strftime('%Y-%m-%d')

print (start_date, end_date)
main_data_dict = {}

###HMBR use get_gl_details_project function##############
df_sales_1 = get_sales_COGS(zid_trade,start_date,end_date)
df_sales_1 = df_sales_1.groupby(['xitem','xdesc'])['totalvalue','xlineamt'].sum().reset_index().round(1)
df_return_1 = get_return(zid_trade,start_date,end_date)
df_return_1 = df_return_1.groupby(['xitem'])['returnvalue','totamt'].sum().reset_index().round(1)
df_final_1 = df_sales_1.merge(df_return_1[['xitem','returnvalue','totamt']],on=['xitem'],how='left').fillna(0)
df_final_1['final_sales'] = df_final_1['xlineamt'] - df_final_1['totamt']
df_final_1['final_cost'] = df_final_1['totalvalue'] + df_final_1['returnvalue']
df_final_1 = df_final_1.drop(['xlineamt','totamt'],axis=1)
df_final_1 = df_final_1.drop(['returnvalue','totalvalue'],axis=1)
df_final_1['Gross_Profit'] = df_final_1['final_sales'] + df_final_1['final_cost']
df_final_1['Profit_Ratio'] = (df_final_1['Gross_Profit']/df_final_1['final_cost'])*-100
df_final_1 = df_final_1.sort_values(by=['Profit_Ratio']).reset_index(drop=True) ########added
df_final_1.loc[len(df_final_1.index),:]=df_final_1.sum(axis=0,numeric_only = True)
df_final_1.at[len(df_final_1.index)-1,'xdesc'] = 'Total_Item_Profit'
df_pl_1 = get_gl_details_project(zid_trade,project_trade,start_date,end_date,COGS)
summary_1 = df_final_1.tail(1).drop('xitem',axis=1)
summary_1 = summary_1.to_dict('records')
df_pl_1 = df_pl_1.to_dict('records')
summary_1[0]['Income_gl'] = df_pl_1[0]['sum']
try:
    summary_1[0]['Expenditure_gl'] = df_pl_1[1]['sum']
except:
    summary_1[0]['Expenditure_gl'] = 0
# summary_1[0]['Expenditure_gl'] = df_pl_1[1]['sum']
main_data_dict[zid_trade] = summary_1[0]
main_data_dict[zid_trade]['Net']=main_data_dict[zid_trade]['Gross_Profit']-main_data_dict[zid_trade]['Expenditure_gl'] ######added
df_pl_1 = pd.DataFrame(df_pl_1)


df_final_1 = pd.concat([df_final_1, df_pl_1],axis=1)

main_data_dict['HMBR'] = main_data_dict.pop(100001)  ####### added



###Karigor use get_gl_details_project function##############
df_sales_0 = get_sales_COGS(zid_karigor,start_date,end_date)
df_sales_0 = df_sales_0.groupby(['xitem','xdesc'])['totalvalue','xlineamt'].sum().reset_index().round(1)
df_return_0 = get_return(zid_karigor,start_date,end_date)
df_return_0 = df_return_0.groupby(['xitem'])['returnvalue','totamt'].sum().reset_index().round(1)
print (df_return_0)
try:
    df_final_0 = df_sales_0.merge(df_return_0[['xitem','returnvalue','totamt']],on=['xitem'],how='left').fillna(0)
    df_final_0['final_sales'] = df_final_0['xlineamt'] - df_final_0['totamt']
    df_final_0['final_cost'] = df_final_0['totalvalue'] + df_final_0['returnvalue']
    df_final_0 = df_final_0.drop(['xlineamt','totamt'],axis=1)
    df_final_0 = df_final_0.drop(['returnvalue','totalvalue'],axis=1)
    df_final_0['Gross_Profit'] = df_final_0['final_sales'] + df_final_0['final_cost']
    df_final_0['Profit_Ratio'] = (df_final_0['Gross_Profit']/df_final_0['final_cost'])*-100
    df_final_0 = df_final_0.sort_values(by=['Profit_Ratio']).reset_index(drop=True) ########added
    df_final_0.loc[len(df_final_0.index),:]=df_final_0.sum(axis=0,numeric_only = True)

    df_final_0.at[len(df_final_0.index)-1,'xdesc'] = 'Total_Item_Profit'


    df_pl_0 = get_gl_details_project(zid_karigor,project_karigor,start_date,end_date,COGS)
    summary_0 = df_final_0.tail(1).drop('xitem',axis=1)
    summary_0 = summary_0.to_dict('records')
    df_pl_0 = df_pl_0.to_dict('records')
    summary_0[0]['Income_gl'] = df_pl_0[0]['sum']
    try:
        summary_0[0]['Expenditure_gl'] = df_pl_0[1]['sum']
    except:
        summary_0[0]['Expenditure_gl'] = 0
    # summary_0[0]['Expenditure_gl'] = df_pl_0[1]['sum']
    main_data_dict[zid_karigor] = summary_0[0]
    main_data_dict[zid_karigor]['Net']=main_data_dict[zid_karigor]['Gross_Profit']-main_data_dict[zid_karigor]['Expenditure_gl'] ######added
    df_pl_0 = pd.DataFrame(df_pl_0)
    df_final_0 = pd.concat([df_final_0, df_pl_0],axis=1)
    main_data_dict['Karigor'] = main_data_dict.pop(100000)  ####### added
except:
    pass
###chemical
df_sales_2 = get_sales_COGS(zid_list[0],start_date,end_date)
df_sales_2 = df_sales_2.groupby(['xitem','xdesc'])['totalvalue','xlineamt'].sum().reset_index().round(1)
df_return_2 = get_return(zid_list[0],start_date,end_date)
df_return_2 = df_return_2.groupby(['xitem'])['totamt'].sum().reset_index().round(1)
df_final_2 = df_sales_2.merge(df_return_2[['xitem','totamt']],on=['xitem'],how='left').fillna(0)
df_final_2['final_sales'] = df_final_2['xlineamt'] - df_final_2['totamt']
df_final_2['final_cost'] = df_final_2['totalvalue'] 
df_final_2 = df_final_2.drop(['xlineamt','totamt'],axis=1)
df_final_2 = df_final_2.drop(['totalvalue'],axis=1)
df_final_2['Gross_Profit'] = df_final_2['final_sales'] + df_final_2['final_cost']
df_final_2['Profit_Ratio'] = (df_final_2['Gross_Profit']/df_final_2['final_cost'])*-100
df_final_2 = df_final_2.sort_values(by='Profit_Ratio').reset_index(drop=True) ########added
df_final_2.loc[len(df_final_2.index),:]=df_final_2.sum(axis=0,numeric_only = True)
df_final_2.at[len(df_final_2.index)-1,'xdesc'] = 'Total_Item_Profit'
df_pl_2 = get_gl_details(zid_list[0],COGS,start_date,end_date)
summary_2 = df_final_2.tail(1).drop('xitem',axis=1)
summary_2 = summary_2.to_dict('records')
df_pl_2 = df_pl_2.to_dict('records')
summary_2[0]['Income_gl'] = df_pl_2[0]['sum']
try:
    summary_2[0]['Expenditure_gl'] = df_pl_2[1]['sum']
except:
    summary_2[0]['Expenditure_gl'] = 0
# summary_2[0]['Expenditure_gl'] = df_pl_2[1]['sum']
main_data_dict[zid_list[0]] = summary_2[0]
main_data_dict[zid_list[0]]['Net']=main_data_dict[zid_list[0]]['Gross_Profit']-main_data_dict[zid_list[0]]['Expenditure_gl'] ######added
df_pl_2 = pd.DataFrame(df_pl_2)
df_final_2 = pd.concat([df_final_2, df_pl_2],axis=1)
main_data_dict['Chemical'] = main_data_dict.pop(100002)  ####### added

##thread tape
df_sales_3 = get_sales_COGS(zid_list[1],start_date,end_date)
df_sales_3 = df_sales_3.groupby(['xitem','xdesc'])['totalvalue','xlineamt'].sum().reset_index().round(1)
df_return_3 = get_return(zid_list[1],start_date,end_date)
df_return_3 = df_return_3.groupby(['xitem'])['totamt'].sum().reset_index().round(1)
df_final_3 = df_sales_3.merge(df_return_3[['xitem','totamt']],on=['xitem'],how='left').fillna(0)
df_final_3['final_sales'] = df_final_3['xlineamt'] - df_final_3['totamt']
df_final_3['final_cost'] = df_final_3['totalvalue'] 
df_final_3 = df_final_3.drop(['xlineamt','totamt'],axis=1)
df_final_3 = df_final_3.drop(['totalvalue'],axis=1)
df_final_3['Gross_Profit'] = df_final_3['final_sales'] + df_final_3['final_cost']
df_final_3['Profit_Ratio'] = (df_final_3['Gross_Profit']/df_final_3['final_cost'])*-100
df_final_3 = df_final_3.sort_values(by=['Profit_Ratio']).reset_index(drop=True) ########added
df_final_3.loc[len(df_final_3.index),:]=df_final_3.sum(axis=0,numeric_only = True)
df_final_3.at[len(df_final_3.index)-1,'xdesc'] = 'Total_Item_Profit'
df_pl_3 = get_gl_details(zid_list[1],COGS,start_date,end_date)
summary_3 = df_final_3.tail(1).drop('xitem',axis=1)
summary_3 = summary_3.to_dict('records')
df_pl_3 = df_pl_3.to_dict('records')
summary_3[0]['Income_gl'] = df_pl_3[0]['sum']
try:
    summary_3[0]['Expenditure_gl'] = df_pl_3[1]['sum']
except:
    summary_3[0]['Expenditure_gl'] = 0
# summary_3[0]['Expenditure_gl'] = df_pl_3[1]['sum']
main_data_dict[zid_list[1]] = summary_3[0]
main_data_dict[zid_list[1]]['Net']=main_data_dict[zid_list[1]]['Gross_Profit']-main_data_dict[zid_list[1]]['Expenditure_gl'] ######added
df_pl_3 = pd.DataFrame(df_pl_3)
df_final_3 = pd.concat([df_final_3, df_pl_3],axis=1)
main_data_dict['ThreadTape'] = main_data_dict.pop(100003)  ####### added

###Plastic  use get_gl_details_project function##############
df_sales_4 = get_sales_COGS(zid_plastic,start_date,end_date)
df_sales_4 = df_sales_4.groupby(['xitem','xdesc'])['totalvalue','xlineamt'].sum().reset_index().round(1)
df_return_4 = get_return(zid_plastic,start_date,end_date)
df_return_4 = df_return_4.groupby(['xitem'])['totamt'].sum().reset_index().round(1)
df_final_4 = df_sales_4.merge(df_return_4[['xitem','totamt']],on=['xitem'],how='left').fillna(0)
df_final_4['final_sales'] = df_final_4['xlineamt'] - df_final_4['totamt']
df_final_4['final_cost'] = df_final_4['totalvalue'] 
df_final_4 = df_final_4.drop(['xlineamt','totamt'],axis=1)
df_final_4 = df_final_4.drop(['totalvalue'],axis=1)
df_final_4['Gross_Profit'] = df_final_4['final_sales'] + df_final_4['final_cost']
df_final_4['Profit_Ratio'] = (df_final_4['Gross_Profit']/df_final_4['final_cost'])*-100
df_final_4 = df_final_4.sort_values(by=['Profit_Ratio']).reset_index(drop=True) ########added
df_final_4.loc[len(df_final_4.index),:]=df_final_4.sum(axis=0,numeric_only = True)
df_final_4.at[len(df_final_4.index)-1,'xdesc'] = 'Total_Item_Profit'
df_pl_4 = get_gl_details_project(zid_plastic,project_plastic,start_date,end_date, COGS)
summary_4 = df_final_4.tail(1).drop('xitem',axis=1)
summary_4 = summary_4.to_dict('records')
df_pl_4 = df_pl_4.to_dict('records')
summary_4[0]['Income_gl'] = df_pl_4[0]['sum']
try:
    summary_4[0]['Expenditure_gl'] = df_pl_4[1]['sum']
except:
    summary_4[0]['Expenditure_gl'] = 0
# summary_4[0]['Expenditure_gl'] = df_pl_4[1]['sum']
main_data_dict[zid_plastic] = summary_4[0]
main_data_dict[zid_plastic]['Net']=main_data_dict[zid_plastic]['Gross_Profit']-main_data_dict[zid_plastic]['Expenditure_gl'] ######added
df_pl_4 = pd.DataFrame(df_pl_4)
df_final_4 = pd.concat([df_final_4, df_pl_4],axis=1)
main_data_dict['Plastic'] = main_data_dict.pop(100004)  ####### added


############ZEPTO ################
######    xlineamt to xdtwotax, COGS should be COGS_zepto
df_sales_5 = get_sales_COGS(zid_list[2],start_date,end_date)
df_sales_5 = df_sales_5.groupby(['xitem','xdesc'])['totalvalue','xdtwotax'].sum().reset_index().round(1)
df_return_5 = get_return(zid_list[2],start_date,end_date)
df_return_5 = df_return_5.groupby(['xitem'])['returnvalue','totamt'].sum().reset_index().round(1)
df_final_5 = df_sales_5.merge(df_return_5[['xitem','returnvalue','totamt']],on=['xitem'],how='left').fillna(0)
df_final_5['final_sales'] = df_final_5['xdtwotax'] - df_final_5['totamt']
df_final_5['final_cost'] = df_final_5['totalvalue'] + df_final_5['returnvalue']
df_final_5 = df_final_5.drop(['xdtwotax','totamt'],axis=1)
df_final_5 = df_final_5.drop(['returnvalue','totalvalue'],axis=1)
df_final_5['Gross_Profit'] = df_final_5['final_sales'] + df_final_5['final_cost']
df_final_5['Profit_Ratio'] = (df_final_5['Gross_Profit']/df_final_5['final_cost'])*-100
df_final_5 = df_final_5.sort_values(by=['Profit_Ratio']).reset_index(drop=True) ########added
df_final_5.loc[len(df_final_5.index),:]=df_final_5.sum(axis=0,numeric_only = True)
df_final_5.at[len(df_final_5.index)-1,'xdesc'] = 'Total_Item_Profit'
df_pl_5 = get_gl_details_zepto(zid_list[2],COGS_zepto,'07080001',start_date,end_date)
summary_5 = df_final_5.tail(1).drop('xitem',axis=1)
summary_5 = summary_5.to_dict('records')
df_pl_5 = df_pl_5.to_dict('records')
summary_5[0]['Income_gl'] = df_pl_5[0]['sum']
try:
    summary_5[0]['Expenditure_gl'] = df_pl_5[1]['sum']
except:
    summary_5[0]['Expenditure_gl'] = 0
# summary_5[0]['Expenditure_gl'] = df_pl_5[1]['sum']
main_data_dict[zid_list[2]] = summary_5[0]
main_data_dict[zid_list[2]]['Net']=main_data_dict[zid_list[2]]['Gross_Profit']-main_data_dict[zid_list[2]]['Expenditure_gl'] ######added
df_pl_5 = pd.DataFrame(df_pl_5)
df_final_5 = pd.concat([df_final_5, df_pl_5],axis=1)
main_data_dict['Zepto'] = main_data_dict.pop(100005)  ####### added

################# PAINT ROLLER ######################
df_sales_7 = get_sales_COGS(zid_list[4],start_date,end_date)
df_sales_7 = df_sales_7.groupby(['xitem','xdesc'])['totalvalue','xlineamt'].sum().reset_index().round(1)
df_return_7 = get_return(zid_list[4],start_date,end_date)
df_return_7 = df_return_7.groupby(['xitem'])['totamt'].sum().reset_index().round(1)
df_final_7 = df_sales_7.merge(df_return_7[['xitem','totamt']],on=['xitem'],how='left').fillna(0)
df_final_7['final_sales'] = df_final_7['xlineamt'] - df_final_7['totamt']
df_final_7['final_cost'] = df_final_7['totalvalue'] 
df_final_7 = df_final_7.drop(['xlineamt','totamt'],axis=1)
df_final_7 = df_final_7.drop(['totalvalue'],axis=1)
df_final_7['Gross_Profit'] = df_final_7['final_sales'] + df_final_7['final_cost']
df_final_7['Profit_Ratio'] = (df_final_7['Gross_Profit']/df_final_7['final_cost'])*-100
df_final_7 = df_final_7.sort_values(by=['Profit_Ratio']).reset_index(drop=True) ########added
df_final_7.loc[len(df_final_7.index),:]=df_final_7.sum(axis=0,numeric_only = True)
df_final_7.at[len(df_final_7.index)-1,'xdesc'] = 'Total_Item_Profit'
df_pl_7 = get_gl_details(zid_list[4],COGS,start_date,end_date)
summary_7 = df_final_7.tail(1).drop('xitem',axis=1)
summary_7 = summary_7.to_dict('records')
df_pl_7 = df_pl_7.to_dict('records')
print (df_pl_7, summary_7)
summary_7[0]['Income_gl'] = df_pl_7[0]['sum']
try:
    summary_7[0]['Expenditure_gl'] = df_pl_7[1]['sum']
except:
    summary_7[0]['Expenditure_gl'] = 0
main_data_dict[zid_list[4]] = summary_7[0]
main_data_dict[zid_list[4]]['Net']=main_data_dict[zid_list[4]]['Gross_Profit']-main_data_dict[zid_list[4]]['Expenditure_gl'] ######added
df_pl_7 = pd.DataFrame(df_pl_7)
df_final_7 = pd.concat([df_final_7, df_pl_7],axis=1)
main_data_dict['PaintRoller'] = main_data_dict.pop(100007)  ####### added

########### Steel Scrubber #############
df_sales_8 = get_sales_COGS(zid_list[5],start_date,end_date)
df_sales_8 = df_sales_8.groupby(['xitem','xdesc'])['totalvalue','xlineamt'].sum().reset_index().round(1)
df_return_8 = get_return(zid_list[5],start_date,end_date)
df_return_8 = df_return_8.groupby(['xitem'])['totamt'].sum().reset_index().round(1)
df_final_8 = df_sales_8.merge(df_return_8[['xitem','totamt']],on=['xitem'],how='left').fillna(0)
df_final_8['final_sales'] = df_final_8['xlineamt'] - df_final_8['totamt']
df_final_8['final_cost'] = df_final_8['totalvalue'] 
df_final_8 = df_final_8.drop(['xlineamt','totamt'],axis=1)
df_final_8 = df_final_8.drop(['totalvalue'],axis=1)
df_final_8['Gross_Profit'] = df_final_8['final_sales'] + df_final_8['final_cost']
df_final_8['Profit_Ratio'] = (df_final_8['Gross_Profit']/df_final_8['final_cost'])*-100
df_final_8 = df_final_8.sort_values(by=['Profit_Ratio']).reset_index(drop=True) ########added
df_final_8.loc[len(df_final_8.index),:]=df_final_8.sum(axis=0,numeric_only = True)
df_final_8.at[len(df_final_8.index)-1,'xdesc'] = 'Total_Item_Profit'
df_pl_8 = get_gl_details(zid_list[5],COGS,start_date,end_date)
summary_8 = df_final_8.tail(1).drop('xitem',axis=1)
summary_8 = summary_8.to_dict('records')
df_pl_8 = df_pl_8.to_dict('records')
summary_8[0]['Income_gl'] = df_pl_8[0]['sum']
try:
    summary_8[0]['Expenditure_gl'] = df_pl_8[1]['sum']
except:
    summary_8[0]['Expenditure_gl'] = 0
# summary_8[0]['Expenditure_gl'] = df_pl_8[1]['sum']
main_data_dict[zid_list[5]] = summary_8[0]
main_data_dict[zid_list[5]]['Net']=main_data_dict[zid_list[5]]['Gross_Profit']-main_data_dict[zid_list[5]]['Expenditure_gl'] ######added
df_pl_8 = pd.DataFrame(df_pl_8)
df_final_8 = pd.concat([df_final_8, df_pl_8],axis=1)
main_data_dict['SteelScrubber'] = main_data_dict.pop(100008)  ####### added

#########################  Packaging ####################
df_sales_9 = get_sales_COGS(zid_list[6],start_date,end_date)
df_sales_9 = df_sales_9.groupby(['xitem','xdesc'])['totalvalue','xlineamt'].sum().reset_index().round(1)
df_return_9 = get_return(zid_list[6],start_date,end_date)
df_return_9 = df_return_9.groupby(['xitem'])['totamt'].sum().reset_index().round(1)
df_final_9 = df_sales_9.merge(df_return_9[['xitem','totamt']],on=['xitem'],how='left').fillna(0)
df_final_9['final_sales'] = df_final_9['xlineamt'] - df_final_9['totamt']
df_final_9['final_cost'] = df_final_9['totalvalue'] 
df_final_9 = df_final_9.drop(['xlineamt','totamt'],axis=1)
df_final_9 = df_final_9.drop(['totalvalue'],axis=1)
df_final_9['Gross_Profit'] = df_final_9['final_sales'] + df_final_9['final_cost']
df_final_9['Profit_Ratio'] = (df_final_9['Gross_Profit']/df_final_9['final_cost'])*-100
df_final_9 = df_final_9.sort_values(by=['Profit_Ratio']).reset_index(drop=True) ########added
df_final_9.loc[len(df_final_9.index),:]=df_final_9.sum(axis=0,numeric_only = True)
df_final_9.at[len(df_final_9.index)-1,'xdesc'] = 'Total_Item_Profit'
df_pl_9 = get_gl_details(zid_list[6],COGS,start_date,end_date)
summary_9 = df_final_9.tail(1).drop('xitem',axis=1)
summary_9 = summary_9.to_dict('records')
df_pl_9 = df_pl_9.to_dict('records')
summary_9[0]['Income_gl'] = df_pl_9[0]['sum']
try:
    summary_9[0]['Expenditure_gl'] = df_pl_9[1]['sum']
except:
    summary_9[0]['Expenditure_gl'] = 0
# summary_9[0]['Expenditure_gl'] = df_pl_9[1]['sum']
main_data_dict[zid_list[6]] = summary_9[0]
main_data_dict[zid_list[6]]['Net']=main_data_dict[zid_list[6]]['Gross_Profit']-main_data_dict[zid_list[6]]['Expenditure_gl'] ######added
df_pl_9 = pd.DataFrame(df_pl_9)
df_final_9 = pd.concat([df_final_9, df_pl_9],axis=1)
main_data_dict['Packaging'] = main_data_dict.pop(100009)  ####### added


#html
#main_data_dict turn into dataframe then html
#remove xdesc key
for key,value in main_data_dict.items():
     (main_data_dict[key].pop('xdesc'))
df_overall= pd.DataFrame.from_dict(main_data_dict).reset_index()

try:
    df_overall_1 = df_overall[['index','Karigor','HMBR','Chemical','ThreadTape','Plastic']].round(2)
    df_overall_2 = df_overall[['index','Zepto','PaintRoller','SteelScrubber','Packaging']].round(2)
except:
    df_overall_1 = df_overall[['index','HMBR','Chemical','ThreadTape','Plastic']].round(2)
    df_overall_2 = df_overall[['index','Zepto','PaintRoller','SteelScrubber','Packaging']].round(2)
# df_overall.loc[len(df_overall.index)] = [i for i in df_overall.loc[0]] 
# df_overall = df_overall.drop ([0])

#df_final_ and df_pl in the same sheet in separate sheets for each business 
# 

############ Excel File Generate ######################
writer = pd.ExcelWriter('ItemWiseProfit.xlsx')
try:
    df_final_0.to_excel(writer, 'karigor')
except:
    pass
df_final_1.to_excel(writer, 'HMBR')
df_final_2.to_excel(writer, 'Chemical')
df_final_3.to_excel(writer, 'ThreadTape')
df_final_4.to_excel(writer, 'Plastic')
df_final_5.to_excel(writer, 'Zepto')
df_final_7.to_excel(writer, 'PaintRoller')
df_final_8.to_excel(writer, 'SteelScrubber')
df_final_9.to_excel(writer, 'Packaging')
writer.save()
################ Change column width of every sheet in excel file ######################
wb = openpyxl.load_workbook('ItemWiseProfit.xlsx')
for sheet in wb:
    sheet.column_dimensions['C'].width = 45
wb.save('ItemWiseProfit.xlsx')

########### Generate html file  ##############################
with open("index.html",'w') as f:

    f.write("<h2 style='color:red;'>All Business Overall Sales and Accounts  </h2>")
    f.write(df_overall_1.to_html(classes='df_overall_1'))
    f.write("<h2 style='color:red;'>Part 2 </h2>")
    f.write(df_overall_2.to_html(classes='df_overall_2'))
me = "pythonhmbr12@gmail.com"
# you = ["asaddat87@gmail.com", "ithmbrbd@gmail.com", "motiurhmbr@gmail.com","admhmbr@gmail.com"]
# you = ["ithmbrbd@gmail.com","asaddat87@gmail.com"]
you = ["ithmbrbd@gmail.com"]

msg = MIMEMultipart('alternative')
msg['Subject'] = f"All Business Item Wise Profit and Loss from [ {start_date} to {end_date} ] "
msg['From'] = me
msg['To'] = ", ".join(you)

filename = "index.html"
f = open(filename)
attachment = MIMEText(f.read(),'html')
msg.attach(attachment)

part1 = MIMEBase('application', "octet-stream")
part1.set_payload(open("ItemWiseProfit.xlsx", "rb").read())
encoders.encode_base64(part1)
part1.add_header('Content-Disposition', 'attachment; filename="ItemWiseProfit.xlsx"')
msg.attach(part1)

username = 'pythonhmbr12@gmail.com'
password = 'vksikttussvnbqef'

s = smtplib.SMTP('smtp.gmail.com:587')
s.starttls()
s.login(username, password)
s.sendmail(me,you,msg.as_string())
s.quit()