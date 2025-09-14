import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from dotenv import load_dotenv
import warnings

# === Load Environment & Config ===
load_dotenv()
# Zid Settings
ZID_HMBR = int(os.environ["ZID_GULSHAN_TRADING"])
ZID_GI = int(os.environ["ZID_GI"])
ZID_ZEPTO = int(os.environ["ZID_ZEPTO_CHEMICALS"])  # Zepto business
ZID_HMBR_ONLINE = int(os.environ["ZID_HMBR_ONLINE_SHOP"])  # HMBR Online business
ZID_PACKAGING = int(os.environ["ZID_GULSHAN_PACKAGING"])  # Packaging business

# Project settings
PROJECT_TRADING = os.environ["PROJECT_100001"]
PROJECT_GI = os.environ["PROJECT_100000"]   
PROJECT_ZEPTO = os.environ["PROJECT_100005"]
PROJECT_HMBR_ONLINE = os.environ["PROJECT_100007"]
PROJECT_PACKAGING = os.environ["PROJECT_100009"]


PROJECT_ROOT = os.path.dirname(os.getcwd())
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from mail import send_mail, get_email_recipients
from project_config import DATABASE_URL

engine = create_engine(DATABASE_URL)
warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)
pd.set_option('display.float_format', '{:.2f}'.format)


def get_sales_COGS(zid,start_date,end_date):
    df = pd.read_sql("""SELECT caitem.zid,caitem.xitem,caitem.xdesc,caitem.xgitem, (imtrn.xqty*imtrn.xsign) as qty, (imtrn.xval*imtrn.xsign) as totalvalue,
                        opddt.xqty as opddt_qty,opddt.xrate,opddt.xlineamt,(opddt.xdtwotax-opddt.xdtdisc) as xdtwotax  
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


COGS_zepto = '04010020'

COGS = '04010020'

end_date = (datetime.now() - timedelta(days = 2)).strftime('%Y-%m-%d')
start_date = (datetime.now() - timedelta(days = 33)).strftime('%Y-%m-%d')

print (start_date, end_date)
main_data_dict = {}

###HMBR use get_gl_details_project function##############
df_sales_1 = get_sales_COGS(ZID_HMBR,start_date,end_date)
df_sales_1 = df_sales_1.groupby(['xitem','xdesc'])['totalvalue','xlineamt'].sum().reset_index().round(1)
df_return_1 = get_return(ZID_HMBR,start_date,end_date)
df_return_1 = df_return_1.groupby(['xitem'])['returnvalue','totamt'].sum().reset_index().round(1)
df_final_1 = df_sales_1.merge(df_return_1[['xitem','returnvalue','totamt']],on=['xitem'],how='left').fillna(0)
df_final_1['final_sales'] = df_final_1['xlineamt'] - df_final_1['totamt']
df_final_1['final_cost'] = df_final_1['totalvalue'] + df_final_1['returnvalue']
df_final_1 = df_final_1.drop(['xlineamt','totamt'],axis=1)
df_final_1 = df_final_1.drop(['returnvalue','totalvalue'],axis=1)
df_final_1['Gross_Profit'] = df_final_1['final_sales'] + df_final_1['final_cost']
df_final_1['Profit_Ratio'] = (df_final_1['Gross_Profit'] / df_final_1['final_sales']) * 100
df_final_1 = df_final_1.sort_values(by=['Profit_Ratio']).reset_index(drop=True) ########added
df_final_1.loc[len(df_final_1.index),:]=df_final_1.sum(axis=0,numeric_only = True)
df_final_1.at[len(df_final_1.index)-1,'xdesc'] = 'Total_Item_Profit'
df_pl_1 = get_gl_details_project(ZID_HMBR, f'{PROJECT_TRADING}',start_date,end_date,COGS)
summary_1 = df_final_1.tail(1).drop('xitem',axis=1)
summary_1['Profit_Ratio'] = (summary_1['Gross_Profit']/summary_1['final_sales']) *100
summary_1 = summary_1.to_dict('records')
df_pl_1 = df_pl_1.to_dict('records')
summary_1[0]['Income_gl'] = df_pl_1[0]['sum']
try:
    summary_1[0]['Expenditure_gl'] = df_pl_1[1]['sum']
except:
    summary_1[0]['Expenditure_gl'] = 0
# summary_1[0]['Expenditure_gl'] = df_pl_1[1]['sum']
main_data_dict[ZID_HMBR] = summary_1[0]
main_data_dict[ZID_HMBR]['Net']=main_data_dict[ZID_HMBR]['Gross_Profit']-main_data_dict[ZID_HMBR]['Expenditure_gl'] ######added
df_pl_1 = pd.DataFrame(df_pl_1)
df_final_1 = pd.concat([df_final_1, df_pl_1],axis=1)
main_data_dict['HMBR'] = main_data_dict.pop(100001)  ####### added



###GI Corporation use get_gl_details_project function##############
df_sales_0 = get_sales_COGS(ZID_GI,start_date,end_date)
df_sales_0 = df_sales_0.groupby(['xitem','xdesc'])['totalvalue','xlineamt'].sum().reset_index().round(1)
df_return_0 = get_return(ZID_GI,start_date,end_date)
df_return_0 = df_return_0.groupby(['xitem'])['returnvalue','totamt'].sum().reset_index().round(1)

try:
    df_final_0 = df_sales_0.merge(df_return_0[['xitem','returnvalue','totamt']],on=['xitem'],how='left').fillna(0)
    df_final_0['final_sales'] = df_final_0['xlineamt'] - df_final_0['totamt']
    df_final_0['final_cost'] = df_final_0['totalvalue'] + df_final_0['returnvalue']
    df_final_0 = df_final_0.drop(['xlineamt','totamt'],axis=1)
    df_final_0 = df_final_0.drop(['returnvalue','totalvalue'],axis=1)
    df_final_0['Gross_Profit'] = df_final_0['final_sales'] + df_final_0['final_cost']
    df_final_0['Profit_Ratio'] = (df_final_0['Gross_Profit']/df_final_0['final_sales'])*100
    df_final_0 = df_final_0.sort_values(by=['Profit_Ratio']).reset_index(drop=True) ########added
    df_final_0.loc[len(df_final_0.index),:]=df_final_0.sum(axis=0,numeric_only = True)

    df_final_0.at[len(df_final_0.index)-1,'xdesc'] = 'Total_Item_Profit'


    df_pl_0 = get_gl_details_project(ZID_GI,PROJECT_GI,start_date,end_date,COGS)
    summary_0 = df_final_0.tail(1).drop('xitem',axis=1)
    summary_0['Profit_Ratio'] = (summary_0['Gross_Profit']/summary_0['final_sales']) *100
    summary_0 = summary_0.to_dict('records')
    df_pl_0 = df_pl_0.to_dict('records')
    summary_0[0]['Income_gl'] = df_pl_0[0]['sum']
    try:
        summary_0[0]['Expenditure_gl'] = df_pl_0[1]['sum']
    except:
        summary_0[0]['Expenditure_gl'] = 0
    # summary_0[0]['Expenditure_gl'] = df_pl_0[1]['sum']
    main_data_dict[ZID_GI] = summary_0[0]
    main_data_dict[ZID_GI]['Net']=main_data_dict[ZID_GI]['Gross_Profit']-main_data_dict[ZID_GI]['Expenditure_gl'] ######added
    df_pl_0 = pd.DataFrame(df_pl_0)
    df_final_0 = pd.concat([df_final_0, df_pl_0],axis=1)
    main_data_dict["GI Corporation"] = main_data_dict.pop(100000)  ####### added  
except:
    pass

print (main_data_dict)



############ZEPTO ################
######    xlineamt to xdtwotax, COGS should be COGS_zepto
df_sales_5 = get_sales_COGS(ZID_ZEPTO,start_date,end_date)
df_sales_5 = df_sales_5.groupby(['xitem','xdesc'])['totalvalue','xdtwotax'].sum().reset_index().round(1)
df_return_5 = get_return(ZID_ZEPTO,start_date,end_date)
df_return_5 = df_return_5.groupby(['xitem'])['returnvalue','totamt'].sum().reset_index().round(1)
df_final_5 = df_sales_5.merge(df_return_5[['xitem','returnvalue','totamt']],on=['xitem'],how='left').fillna(0)
df_final_5['final_sales'] = df_final_5['xdtwotax'] - df_final_5['totamt']
df_final_5['final_cost'] = df_final_5['totalvalue'] + df_final_5['returnvalue']
df_final_5 = df_final_5.drop(['xdtwotax','totamt'],axis=1)
df_final_5 = df_final_5.drop(['returnvalue','totalvalue'],axis=1)
df_final_5['Gross_Profit'] = df_final_5['final_sales'] + df_final_5['final_cost']
df_final_5['Profit_Ratio'] = (df_final_5['Gross_Profit']/df_final_5['final_sales'])*100
df_final_5 = df_final_5.sort_values(by=['Profit_Ratio']).reset_index(drop=True) ########added
df_final_5.loc[len(df_final_5.index),:]=df_final_5.sum(axis=0,numeric_only = True)
df_final_5.at[len(df_final_5.index)-1,'xdesc'] = 'Total_Item_Profit'
df_pl_5 = get_gl_details_zepto(ZID_ZEPTO,COGS_zepto,'07080001',start_date,end_date)
summary_5 = df_final_5.tail(1).drop('xitem',axis=1)
summary_5['Profit_Ratio'] = (summary_5['Gross_Profit']/summary_5['final_sales']) *100
summary_5 = summary_5.to_dict('records')
df_pl_5 = df_pl_5.to_dict('records')
summary_5[0]['Income_gl'] = df_pl_5[0]['sum']
try:
    summary_5[0]['Expenditure_gl'] = df_pl_5[1]['sum']
except:
    summary_5[0]['Expenditure_gl'] = 0
# summary_5[0]['Expenditure_gl'] = df_pl_5[1]['sum']
main_data_dict[ZID_ZEPTO] = summary_5[0]
main_data_dict[ZID_ZEPTO]['Net']=main_data_dict[ZID_ZEPTO]['Gross_Profit']-main_data_dict[ZID_ZEPTO]['Expenditure_gl'] ######added
df_pl_5 = pd.DataFrame(df_pl_5)
df_final_5 = pd.concat([df_final_5, df_pl_5],axis=1)
main_data_dict["Zepto"] = main_data_dict.pop(100005)  ####### added

################# HMBR Online Shop ######################
df_sales_7 = get_sales_COGS(ZID_HMBR_ONLINE,start_date,end_date)
df_sales_7 = df_sales_7.groupby(['xitem','xdesc'])['totalvalue','xlineamt'].sum().reset_index().round(1)
df_return_7 = get_return(ZID_HMBR_ONLINE,start_date,end_date)
df_return_7 = df_return_7.groupby(['xitem'])['totamt'].sum().reset_index().round(1)
df_final_7 = df_sales_7.merge(df_return_7[['xitem','totamt']],on=['xitem'],how='left').fillna(0)
df_final_7['final_sales'] = df_final_7['xlineamt'] - df_final_7['totamt']
df_final_7['final_cost'] = df_final_7['totalvalue'] 
df_final_7 = df_final_7.drop(['xlineamt','totamt'],axis=1)
df_final_7 = df_final_7.drop(['totalvalue'],axis=1)
df_final_7['Gross_Profit'] = df_final_7['final_sales'] + df_final_7['final_cost']
df_final_7['Profit_Ratio'] = (df_final_7['Gross_Profit']/df_final_7['final_sales'])*100
df_final_7 = df_final_7.sort_values(by=['Profit_Ratio']).reset_index(drop=True) ########added
df_final_7.loc[len(df_final_7.index),:]=df_final_7.sum(axis=0,numeric_only = True)
df_final_7.at[len(df_final_7.index)-1,'xdesc'] = 'Total_Item_Profit'
df_pl_7 = get_gl_details(ZID_HMBR_ONLINE,COGS,start_date,end_date)
summary_7 = df_final_7.tail(1).drop('xitem',axis=1)
summary_7['Profit_Ratio'] = (summary_7['Gross_Profit']/summary_7['final_sales']) *100
summary_7 = summary_7.to_dict('records')
df_pl_7 = df_pl_7.to_dict('records')
print (df_pl_7, summary_7)
summary_7[0]['Income_gl'] = df_pl_7[0]['sum']
try:
    summary_7[0]['Expenditure_gl'] = df_pl_7[1]['sum']
except:
    summary_7[0]['Expenditure_gl'] = 0
main_data_dict[ZID_HMBR_ONLINE] = summary_7[0]
main_data_dict[ZID_HMBR_ONLINE]['Net']=main_data_dict[ZID_HMBR_ONLINE]['Gross_Profit']-main_data_dict[ZID_HMBR_ONLINE]['Expenditure_gl'] ######added
df_pl_7 = pd.DataFrame(df_pl_7)
df_final_7 = pd.concat([df_final_7, df_pl_7],axis=1)
main_data_dict['hmbr_online_shop'] = main_data_dict.pop(100007)  ####### added


#########################  Packaging ####################
df_sales_9 = get_sales_COGS(ZID_PACKAGING,start_date,end_date)
df_sales_9 = df_sales_9.groupby(['xitem','xdesc'])['totalvalue','xlineamt'].sum().reset_index().round(1)
df_return_9 = get_return(ZID_PACKAGING,start_date,end_date) 
df_return_9 = df_return_9.groupby(['xitem'])['totamt'].sum().reset_index().round(1)
df_final_9 = df_sales_9.merge(df_return_9[['xitem','totamt']],on=['xitem'],how='left').fillna(0)
df_final_9['final_sales'] = df_final_9['xlineamt'] - df_final_9['totamt']
df_final_9['final_cost'] = df_final_9['totalvalue'] 
df_final_9 = df_final_9.drop(['xlineamt','totamt'],axis=1)
df_final_9 = df_final_9.drop(['totalvalue'],axis=1)
df_final_9['Gross_Profit'] = df_final_9['final_sales'] + df_final_9['final_cost']
df_final_9['Profit_Ratio'] = (df_final_9['Gross_Profit']/df_final_9['final_sales'])*100
df_final_9 = df_final_9.sort_values(by=['Profit_Ratio']).reset_index(drop=True) ########added
df_final_9.loc[len(df_final_9.index),:]=df_final_9.sum(axis=0,numeric_only = True)
df_final_9.at[len(df_final_9.index)-1,'xdesc'] = 'Total_Item_Profit'
df_pl_9 = get_gl_details(ZID_PACKAGING,COGS,start_date,end_date)
summary_9 = df_final_9.tail(1).drop('xitem',axis=1)
summary_9['Profit_Ratio'] = (summary_9['Gross_Profit']/summary_9['final_sales']) *100
summary_9 = summary_9.to_dict('records')
df_pl_9 = df_pl_9.to_dict('records')
summary_9[0]['Income_gl'] = df_pl_9[0]['sum']
try:
    summary_9[0]['Expenditure_gl'] = df_pl_9[1]['sum']
except:
    summary_9[0]['Expenditure_gl'] = 0
# summary_9[0]['Expenditure_gl'] = df_pl_9[1]['sum']
main_data_dict[ZID_PACKAGING] = summary_9[0]
main_data_dict[ZID_PACKAGING]['Net']=main_data_dict[ZID_PACKAGING]['Gross_Profit']-main_data_dict[ZID_PACKAGING]['Expenditure_gl'] ######added
df_pl_9 = pd.DataFrame(df_pl_9)
df_final_9 = pd.concat([df_final_9, df_pl_9],axis=1)
main_data_dict['Packaging'] = main_data_dict.pop(100009)  ####### added


print ("Main Data Dict\n\n", main_data_dict)


# combine seperate sheet of every df_final and export it to a single excel file with seperate sheet for every brand
with pd.ExcelWriter('item_wise_profit.xlsx') as writer:
    df_final_1.to_excel(writer, sheet_name='HMBR', index=False)
    df_final_0.to_excel(writer, sheet_name='GI Corp', index=False)
    df_final_5.to_excel(writer, sheet_name='Zepto', index=False)
    df_final_7.to_excel(writer, sheet_name='HMBR_Online_Shop', index=False)
    df_final_9.to_excel(writer, sheet_name='Packaging', index=False)



# Convert dict to DataFrame
data_rows = []
for brand, vals in main_data_dict.items():
    data_rows.append({
        'Brand': brand,
        'Description': vals['xdesc'],
        'Final Sales': f"{vals['final_sales']:,.2f}",
        'Final Cost': f"{vals['final_cost']:,.2f}",
        'Gross Profit': f"{vals['Gross_Profit']:,.2f}",
        'Profit Ratio (%)': f"{vals['Profit_Ratio']:,.2f}",
        'Income (GL)': f"{vals['Income_gl']:,.2f}",
        'Expenditure (GL)': f"{vals['Expenditure_gl']:,.2f}",
        'Net': f"{vals['Net']:,.2f}"
    })

df = pd.DataFrame(data_rows)



# Send email with HTML table
send_mail(
    recipient=["ithmbrbd@gmail.com"],
    subject=f"Daily Item-Wise Profit Summary {start_date} to {end_date}",
    bodyText="Please find the item-wise profit summary below:",
    attachment=['item_wise_profit.xlsx'],
    html_body=[(df, f"Item-Wise Profit Summary Report {start_date} to {end_date}")]
)