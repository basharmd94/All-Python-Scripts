# %%
# in this new script whe remove plastic, scrubber, chemical section and rename GI_Corp to gi

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
# pd.set_option('display.float_format', lambda x: '%.3f' % x)
########4###########################

def get_gl_details(zid,year,smonth,emonth):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    df = pd.read_sql("""select glmst.zid, glmst.xacc,glheader.xyear, glheader.xper,SUM(gldetail.xprime)
                        FROM glmst
                        JOIN
                        gldetail
                        ON glmst.xacc = gldetail.xacc
                        JOIN
                        glheader
                        ON gldetail.xvoucher = glheader.xvoucher
                        WHERE glmst.zid = '%s'
                        AND gldetail.zid = '%s'
                        AND glheader.zid = '%s'
                        AND (glmst.xacctype = 'Income' OR glmst.xacctype = 'Expenditure')
                        AND glheader.xyear = '%s'
                        AND glheader.xper >= '%s'
                        AND glheader.xper <= '%s'
                        GROUP BY glmst.zid, glmst.xacc, glmst.xacctype, glmst.xhrc1, glmst.xhrc2, glheader.xyear, glheader.xper
                        ORDER BY glheader.xper ASC , glmst.xacctype"""%(zid,zid,zid,year,smonth,emonth),con = engine)
    return df

def get_gl_details_project(zid,project,year,smonth,emonth):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    df = pd.read_sql("""select glmst.zid, glmst.xacc, glheader.xyear, glheader.xper,SUM(gldetail.xprime)
                        FROM glmst
                        JOIN
                        gldetail
                        ON glmst.xacc = gldetail.xacc
                        JOIN
                        glheader
                        ON gldetail.xvoucher = glheader.xvoucher
                        WHERE glmst.zid = '%s'
                        AND gldetail.zid = '%s'
                        AND glheader.zid = '%s'
                        AND gldetail.xproj = '%s'
                        AND (glmst.xacctype = 'Income' OR glmst.xacctype = 'Expenditure')
                        AND glheader.xyear = '%s'
                        AND glheader.xper >= '%s'
                        AND glheader.xper <= '%s'
                        GROUP BY glmst.zid, glmst.xacc, glmst.xacctype, glmst.xhrc1, glmst.xhrc2, glheader.xyear, glheader.xper
                        ORDER BY glheader.xper ASC , glmst.xacctype"""%(zid,zid,zid,project,year,smonth,emonth),con = engine)
    return df

def get_gl_details_bs(zid,year,emonth):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    df = pd.read_sql("""select glmst.zid, glmst.xacc, glheader.xyear, glheader.xper,SUM(gldetail.xprime)
                        FROM glmst
                        JOIN
                        gldetail
                        ON glmst.xacc = gldetail.xacc
                        JOIN
                        glheader
                        ON gldetail.xvoucher = glheader.xvoucher
                        WHERE glmst.zid = '%s'
                        AND gldetail.zid = '%s'
                        AND glheader.zid = '%s'
                        AND (glmst.xacctype = 'Asset' OR glmst.xacctype = 'Liability')
                        AND glheader.xyear = '%s'
                        AND glheader.xper <= '%s'
                        GROUP BY glmst.zid, glmst.xacc, glheader.xyear, glheader.xper"""%(zid,zid,zid,year,emonth),con = engine)
    return df

def get_gl_details_bs_project(zid,project,year,emonth):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    df = pd.read_sql("""select glmst.zid, glmst.xacc, glmst.xacctype, glmst.xhrc1, glmst.xhrc2, glmst.xaccusage, glheader.xyear, glheader.xper,SUM(gldetail.xprime)
                        FROM glmst
                        JOIN
                        gldetail
                        ON glmst.xacc = gldetail.xacc
                        JOIN
                        glheader
                        ON gldetail.xvoucher = glheader.xvoucher
                        WHERE glmst.zid = '%s'
                        AND gldetail.zid = '%s'
                        AND glheader.zid = '%s'
                        AND gldetail.xproj = '%s'
                        AND (glmst.xacctype = 'Asset' OR glmst.xacctype = 'Liability')
                        AND glheader.xyear = '%s'
                        AND glheader.xper <= '%s'
                        GROUP BY glmst.zid, glmst.xacc, glmst.xacctype, glmst.xhrc1, glmst.xhrc2, glheader.xyear, glheader.xper
                        ORDER BY glheader.xper ASC , glmst.xacctype"""%(zid,zid,zid,project,year,emonth),con = engine)
    return df

def get_gl_master(zid):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    df = pd.read_sql("""SELECT xacc, xdesc, xacctype, xhrc1, xhrc2, xhrc3, xhrc4 FROM glmst WHERE glmst.zid = %s"""%(zid),con=engine)
    return df

def get_gl_details_ap_project(zid,project,year,xacc,emonth,sup_list):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    if isinstance(sup_list,tuple):
        df1 = pd.read_sql("""SELECT 'INTERNAL',SUM(gldetail.xprime)
                            FROM glmst
                            JOIN
                            gldetail
                            ON glmst.xacc = gldetail.xacc
                            JOIN
                            glheader
                            ON gldetail.xvoucher = glheader.xvoucher
                            WHERE glmst.zid = '%s'
                            AND gldetail.zid = '%s'
                            AND glheader.zid = '%s'
                            AND gldetail.xproj = '%s'
                            AND glmst.xacc = '%s' 
                            AND glheader.xyear = '%s'
                            AND glheader.xper <= '%s'
                            AND gldetail.xsub IN %s"""%(zid,zid,zid,project,xacc,year,emonth,sup_list),con = engine)
        df2 = pd.read_sql("""SELECT 'EXTERNAL',SUM(gldetail.xprime)
                            FROM glmst
                            JOIN
                            gldetail
                            ON glmst.xacc = gldetail.xacc
                            JOIN
                            glheader
                            ON gldetail.xvoucher = glheader.xvoucher
                            WHERE glmst.zid = '%s'
                            AND gldetail.zid = '%s'
                            AND glheader.zid = '%s'
                            AND gldetail.xproj = '%s'
                            AND glmst.xacc = '%s' 
                            AND glheader.xyear = '%s'
                            AND glheader.xper <= '%s'
                            AND gldetail.xsub NOT IN %s"""%(zid,zid,zid,project,xacc,year,emonth,sup_list),con = engine)
    else:
        df1 = pd.read_sql("""SELECT 'EXTERNAL',SUM(gldetail.xprime)
                            FROM glmst
                            JOIN
                            gldetail
                            ON glmst.xacc = gldetail.xacc
                            JOIN
                            glheader
                            ON gldetail.xvoucher = glheader.xvoucher
                            WHERE glmst.zid = '%s'
                            AND gldetail.zid = '%s'
                            AND glheader.zid = '%s'
                            AND gldetail.xproj = '%s'
                            AND glmst.xacc = '%s' 
                            AND glheader.xyear = '%s'
                            AND glheader.xper <= '%s'
                            AND gldetail.xsub != '%s'"""%(zid,zid,zid,project,xacc,year,emonth,sup_list),con = engine)
        df2 = pd.read_sql("""SELECT 'INTERNAL',SUM(gldetail.xprime)
                            FROM glmst
                            JOIN
                            gldetail
                            ON glmst.xacc = gldetail.xacc
                            JOIN
                            glheader
                            ON gldetail.xvoucher = glheader.xvoucher
                            WHERE glmst.zid = '%s'
                            AND gldetail.zid = '%s'
                            AND glheader.zid = '%s'
                            AND gldetail.xproj = '%s'
                            AND glmst.xacc = '%s' 
                            AND glheader.xyear = '%s'
                            AND glheader.xper <= '%s'
                            AND gldetail.xsub = '%s'"""%(zid,zid,zid,project,xacc,year,emonth,sup_list),con = engine)
    df = pd.concat([df1,df2],axis=0)
    return df

# %%

ap_dict =  {100000:['GI Corporation','9030001',('SUP-000003','SUP-000004','SUP-000060','SUP-000061')],
            100001:['GULSHAN TRADING','09030001',('SUP-000001','SUP-000002','SUP-000003','SUP-000004','SUP-000010','SUP-000014','SUP-000020','SUP-000027','SUP-000049','SUP-000057')],
            100005:['Zepto Chemicals','09030001',('SUP-000006','SUP-000011','SUP-000012','SUP-000016')],
            100006:['HMBR Grocery Shop','09030001',('SUP-000006','SUP-000003')],
            100009:['Gulshan Packaging','09030001','SUP-000002']}

    


# %%
### define business Id and date time year list for comparison (separate if project)
zid_list_hmbr = [100005,100006,100009]
# zid_list_fixit = [100000,100001,100002,100003]
zid_trade = 100001
zid_plastic = 100004
zid_GI_Corp = 100000

project_trade = 'GULSHAN TRADING'

project_plastic = 'Gulshan Plastic'

project_GI_Corp = 'GI Corporation'
##### call SQL once and get the main data once into a dataframe (get the year and month as an integer)
start_year = int(input('input year like 2022------  '))
start_month = int(input('input from month eg: if january then 1------'))
end_month = int(input('input end month eg: if january then 1------  '))

# %%
### make a 3 year list
year_list = []
new_year = 0
for i in range(5):
    new_year = start_year - i
    year_list.append(new_year)
year_list.reverse()
    
#create master dataframe

    # in order for a proper debug we are going to do sum tests on each part of the project algorithm loop to find our why the merge is not working
    #that is exactly what is not working becuase the data behaves until then. 

# %%

main_data_dict_pl = {}
for i in zid_list_hmbr:
    df_master = get_gl_master(i)
    df_master = df_master[(df_master['xacctype']!='Asset') & (df_master['xacctype']!='Liability')]
#     df_main = get_gl_details(i,start_year,start_month,end_month)
#     df_main = df_main.groupby(['xacc'])['sum'].sum().reset_index().round(1).rename(columns={'sum':start_year})
    for item,idx in enumerate(year_list):
        df = get_gl_details(i,idx,start_month,end_month)
        df = df.groupby(['xacc'])['sum'].sum().reset_index().round(1)
        if item == 0:
#             df_new = df_main.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
            df_new = df_master.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
        else:
            df_new = df_new.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
    main_data_dict_pl[i] = df_new.sort_values(['xacctype'],ascending=True)

# %%
df_master = get_gl_master(zid_trade)
df_master = df_master[(df_master['xacctype']!='Asset') & (df_master['xacctype']!='Liability')]
for item,idx in enumerate(year_list):
    df = get_gl_details_project(zid_trade,project_trade,idx,start_month,end_month)
    df = df.groupby(['xacc'])['sum'].sum().reset_index().round(1)
    if item == 0:
        df_new = df_master.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
    else:
        df_new = df_new.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
main_data_dict_pl[zid_trade] = df_new.sort_values(['xacctype'],ascending=True)

# %%
df_master = get_gl_master(zid_plastic)
df_master = df_master[(df_master['xacctype']!='Asset') & (df_master['xacctype']!='Liability')]
for item,idx in enumerate(year_list):
    df = get_gl_details_project(zid_plastic,project_plastic,idx,start_month,end_month)
    df = df.groupby(['xacc'])['sum'].sum().reset_index().round(1)
    if item == 0:
        df_new = df_master.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
    else:
        df_new = df_new.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
main_data_dict_pl[zid_plastic] = df_new.sort_values(['xacctype'],ascending=True)

# %%
df_master = get_gl_master(zid_GI_Corp)
df_master = df_master[(df_master['xacctype']!='Asset') & (df_master['xacctype']!='Liability')]
for item,idx in enumerate(year_list):
    df = get_gl_details_project(zid_GI_Corp,project_GI_Corp,idx,start_month,end_month)
    df = df.groupby(['xacc'])['sum'].sum().reset_index().round(1)
    print(df['sum'].sum(),'profit & loss')
    if item == 0:
        df_new = df_master.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
    else:
        df_new = df_new.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
    print('kargor work is done')
main_data_dict_pl[zid_GI_Corp] = df_new.sort_values(['xacctype'],ascending=True)

# %%

main_data_dict_bs = {}
for i in zid_list_hmbr:
    df_master = get_gl_master(i)
    df_master = df_master[(df_master['xacctype']!='Income') & (df_master['xacctype']!='Expenditure')]
    for item,idx in enumerate(year_list):
        df = get_gl_details_bs(i,idx,end_month)
        df = df.groupby(['xacc'])['sum'].sum().reset_index().round(1)
        if item == 0:
            df_new = df_master.merge(df[['xacc','sum']],on=['xacc'],how='left').fillna(0).rename(columns={'sum':idx})
        else:
            df_new = df_new.merge(df[['xacc','sum']],on=['xacc'],how='left').fillna(0).rename(columns={'sum':idx})
        main_data_dict_bs[i] = df_new.sort_values(['xacctype'],ascending=True)

# %%
main_data_dict_bs.keys()

# %%
# df_trade_bs = df_trade_bs.groupby(['xacc','xdesc'])['sum'].sum().reset_index().round(1).rename(columns={'sum':start_year})
df_master = get_gl_master(zid_trade)
df_master = df_master[(df_master['xacctype']!='Income') & (df_master['xacctype']!='Expenditure')]
for item,idx in enumerate(year_list):
    df = get_gl_details_bs_project(zid_trade,project_trade,idx,end_month)
    df = df.groupby(['xacc'])['sum'].sum().reset_index().round(1)
    if item == 0:
        df_new = df_master.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
    else:
        df_new = df_new.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
# df_mst = get_gl_master(i)
# df_new = df_new.merge(df_mst,on='xacc',how='left')
main_data_dict_bs[zid_trade] = df_new.sort_values(['xacctype'],ascending=True)

# %%
df_master = get_gl_master(zid_plastic)
df_master = df_master[(df_master['xacctype']!='Income') & (df_master['xacctype']!='Expenditure')]
for item,idx in enumerate(year_list):
    df = get_gl_details_bs_project(zid_plastic,project_plastic,idx,end_month)
    df = df.groupby(['xacc'])['sum'].sum().reset_index().round(1)
    if item == 0:
        df_new = df_master.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
    else:
        df_new = df_new.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
# df_mst = get_gl_master(i)
# df_new = df_new.merge(df_mst,on='xacc',how='left')
# main_data_dict_bs[zid_plastic] = df_new.sort_values(['xacctype'],ascending=True)

# %%
df_master = get_gl_master(zid_GI_Corp)
df_master = df_master[(df_master['xacctype']!='Income') & (df_master['xacctype']!='Expenditure')]
for item,idx in enumerate(year_list):
    df = get_gl_details_bs_project(zid_GI_Corp,project_GI_Corp,idx,end_month)
    df = df.groupby(['xacc'])['sum'].sum().reset_index().round(1)
    if item == 0:
        df_new = df_master.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
    else:
        df_new = df_new.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
# df_mst = get_gl_master(i)
# df_new = df_new.merge(df_mst,on='xacc',how='left')
main_data_dict_bs[zid_GI_Corp] = df_new.sort_values(['xacctype'],ascending=True)

# %%
ap_final_dict = {}
data_ap = {'AP_TYPE':['INTERNAL','EXTERNAL']}

for k,v in ap_dict.items():
    df_ap = pd.DataFrame(data_ap)
    for item,idx in enumerate(year_list):
        zid = k
        project = v[0]
        acc = v[1]
        sup_list = v[2]

        df_1 = get_gl_details_ap_project(zid,project,idx,acc,end_month,sup_list).round(1).rename(columns={'?column?':'AP_TYPE','sum':idx}).fillna(0)

        df_ap = df_ap.merge(df_1,on='AP_TYPE',how='left')
        ap_final_dict[k] = df_ap

# %%

level_1_dict = {}
for key in main_data_dict_pl:
    level_1_dict[key] = main_data_dict_pl[key].groupby(['xacctype'])[[i for i in year_list]].sum().reset_index().round(1)
    level_1_dict[key].loc[len(level_1_dict[key].index),:]=level_1_dict[key].sum(axis=0,numeric_only = True)
    level_1_dict[key].at[len(level_1_dict[key].index)-1,'xacctype'] = 'Profit/Loss'
    ## we can add new ratios right here!
    
level_2_dict = {}
for key in main_data_dict_pl:
    level_2_dict[key] = main_data_dict_pl[key].groupby(['xhrc1'])[[i for i in year_list]].sum().reset_index().round(1)
    level_2_dict[key].loc[len(level_2_dict[key].index),:]=level_2_dict[key].sum(axis=0,numeric_only = True)
    level_2_dict[key].at[len(level_2_dict[key].index)-1,'xhrc1'] = 'Profit/Loss'
    
level_3_dict = {}
for key in main_data_dict_pl:
    level_3_dict[key] = main_data_dict_pl[key].groupby(['xhrc2'])[[i for i in year_list]].sum().reset_index().round(1)
    level_3_dict[key].loc[len(level_3_dict[key].index),:]=level_3_dict[key].sum(axis=0,numeric_only = True)
    level_3_dict[key].at[len(level_3_dict[key].index)-1,'xhrc2'] = 'Profit/Loss'

# %%
income_statement_label = {'04-Cost of Goods Sold':'02-Cost of Revenue',
'0401-DIRECT EXPENSES':'07-Other Operating Expenses, Total',
'0401-PURCHASE':'07-Other Operating Expenses, Total',
'0501-OTHERS DIRECT EXPENSES':'07-Other Operating Expenses, Total',
'0601-OTHERS DIRECT EXPENSES':'07-Other Operating Expenses, Total',
'0631- Development Expenses':'07-Other Operating Expenses, Total',
'06-Office & Administrative Expenses':'03-Office & Administrative Expenses',
'0625-Property Tax & Others':'09-Income Tax & VAT',
'0629- HMBR VAT & Tax Expenses':'09-Income Tax & VAT',
'0629-VAT & Tax Expenses':'09-Income Tax & VAT',
'0630- Bank Interest & Charges':'08-Interest Expense',
'0630-Bank Interest & Charges':'08-Interest Expense',
'0631-Other Expenses':'07-Other Operating Expenses, Total',
'0633-Interest-Loan':'08-Interest Expense',
'0636-Depreciation':'05-Depreciation/Amortization',
'07-Sales & Distribution Expenses':'04-Sales & Distribution Expenses',
'SALES & DISTRIBUTION EXPENSES':'04-Sales & Distribution Expenses',
'08-Revenue':'01-Revenue',
'14-Purchase Return':'06-Unusual Expenses (Income)',
'15-Sales Return':'06-Unusual Expenses (Income)',
'':'06-Unusual Expenses (Income)',
'Profit/Loss':'10-Net Income'}

# %%
income_label = pd.DataFrame(income_statement_label.items(),columns = ['xhrc4','Income Statement'])

level_4_dict = {}
income_s_dict = {}
for key in main_data_dict_pl:
    level_4_dict[key] = main_data_dict_pl[key].groupby(['xhrc4'])[[i for i in year_list]].sum().reset_index().round(1)
    level_4_dict[key].loc[len(level_4_dict[key].index),:]=level_4_dict[key].sum(axis=0,numeric_only = True)
    level_4_dict[key].at[len(level_4_dict[key].index)-1,'xhrc4'] = 'Profit/Loss'
    df = level_4_dict[key].merge(income_label[['xhrc4','Income Statement']],on=['xhrc4'],how='left').sort_values('Income Statement').set_index('Income Statement').reset_index()
    income_s_dict[key] = df.groupby(['Income Statement']).sum().reset_index()
    if ~income_s_dict[key]['Income Statement'].isin(['06-Unusual Expenses (Income)']).any():
        income_s_dict[key].loc[4.5,'Income Statement']= '06-Unusual Expenses (Income)'
        income_s_dict[key] = income_s_dict[key].sort_index().reset_index(drop=True).fillna(0)
    income_s_dict[key].loc[1.5] = income_s_dict[key].loc[0]+income_s_dict[key].loc[1]
    income_s_dict[key].loc[1.5,'Income Statement']= 'Gross Profit'
    income_s_dict[key] = income_s_dict[key].sort_index().reset_index(drop=True)
    income_s_dict[key].loc[7.5] = income_s_dict[key].loc[2] + income_s_dict[key].loc[3] + income_s_dict[key].loc[4] + income_s_dict[key].loc[5] + income_s_dict[key].loc[6] + income_s_dict[key].loc[7]
    income_s_dict[key].loc[7.5,'Income Statement']= 'EBIT'
    income_s_dict[key] = income_s_dict[key].sort_index().reset_index(drop=True)
    income_s_dict[key].loc[9.5] = income_s_dict[key].loc[8] + income_s_dict[key].loc[9]
    income_s_dict[key].loc[9.5,'Income Statement']= 'EBT'
    income_s_dict[key] = income_s_dict[key].sort_index().reset_index(drop=True)
    print('end',len(income_s_dict[key]))

# %%
balance_sheet_label = {
'0101-CASH & CASH EQUIVALENT':'01-Cash',
'0102-BANK BALANCE':'01-Cash',
'0103-ACCOUNTS RECEIVABLE':'02-Accounts Receivable',
'0104-PREPAID EXPENSES':'04-Prepaid Expenses',
'0105-ADVANCE ACCOUNTS':'04-Prepaid Expenses',
'0106-STOCK IN HAND':'03-Inventories',
'02-OTHER ASSET':'05-Other Assets',
'0201-DEFFERED CAPITAL EXPENDITURE':'05-Other Assets',
'0203-LOAN TO OTHERS CONCERN':'05-Other Assets',
'0204-SECURITY DEPOSIT':'05-Other Assets',
'0205-LOAN TO OTHERS CONCERN':'05-Other Assets',
'0206-Other Investment':'05-Other Assets',
'0301-Office Equipment':'06-Property, Plant & Equipment',
'0302-Corporate Office Equipments':'06-Property, Plant & Equipment',
'0303-Furniture & Fixture':'06-Property, Plant & Equipment',
'0304-Trading Vehicles':'06-Property, Plant & Equipment',
'0305-Private Vehicles':'06-Property, Plant & Equipment',
'0306- Plants & Machinery':'06-Property, Plant & Equipment',
'0307-Intangible Asset':'07-Goodwill & Intangible Asset',
'0308-Land & Building':'06-Property, Plant & Equipment',
'0901-Accrued Expenses':'09-Accrued Liabilities',
'0902-Income Tax Payable':'09-Accrued Liabilities',
'0903-Accounts Payable':'08-Accounts Payable',
'0904-Money Agent Liability':'10-Other Short Term Liabilities',
'0904-Reconciliation Liability':'10-Other Short Term Liabilities',
'0905-C & F Liability':'10-Other Short Term Liabilities',
'0906-Others Liability':'10-Other Short Term Liabilities',
'1001-Short Term Bank Loan':'11-Debt',
'1002-Short Term Loan':'11-Debt',
'11-Reserve & Fund':'12-Other Long Term Liabilities',
'1202-Long Term Bank Loan':'11-Debt',
'13-Owners Equity':'13-Total Shareholders Equity'}

# %%
balance_label = pd.DataFrame(balance_sheet_label.items(),columns = ['xhrc4','Balance Sheet'])

level_4_dict_bs = {}
balance_s_dict = {}

# %%
ap_final_dict.keys()

main_data_dict_bs.keys()

main_data_dict_bs.keys()

if 100004 in main_data_dict_bs:
    del main_data_dict_bs[100004]

# Verify the key is removed
print(main_data_dict_bs.keys())

# %%
for key in main_data_dict_bs:
    level_4_dict_bs[key] = main_data_dict_bs[key].groupby(['xhrc4'])[[i for i in year_list]].sum().reset_index().round(1)
    level_4_dict_bs[key].loc[len(level_4_dict_bs[key].index),:]=level_4_dict_bs[key].sum(axis=0,numeric_only = True)
    level_4_dict_bs[key].at[len(level_4_dict_bs[key].index)-1,'xhrc4'] = 'Balance'
    df = level_4_dict_bs[key].merge(balance_label[['xhrc4','Balance Sheet']],on=['xhrc4'],how='left').sort_values('Balance Sheet').set_index('Balance Sheet').reset_index().drop(['xhrc4'],axis=1)
    balance_s_dict[key] = df.groupby(['Balance Sheet']).sum().reset_index()
    df = ap_final_dict[key][ap_final_dict[key]['AP_TYPE']=='EXTERNAL'].rename(columns={'AP_TYPE':'Balance Sheet'})
    balance_s_dict[key] = balance_s_dict[key].append(df).reset_index().drop(['index'],axis=1)
    balance_s_dict[key].loc[-0.5,'Balance Sheet'] = 'Assets'
    balance_s_dict[key] = balance_s_dict[key].sort_index().reset_index(drop=True)
    balance_s_dict[key].loc[0.5,'Balance Sheet'] = 'Current Assets'
    balance_s_dict[key] = balance_s_dict[key].sort_index().reset_index(drop=True)
    balance_s_dict[key].loc[5.5] = balance_s_dict[key].loc[2] + balance_s_dict[key].loc[3] + balance_s_dict[key].loc[4] + balance_s_dict[key].loc[5]
    balance_s_dict[key].loc[5.5,'Balance Sheet'] = 'Total Current Asset'
    balance_s_dict[key] = balance_s_dict[key].sort_index().reset_index(drop=True)
    balance_s_dict[key].loc[6.5,'Balance Sheet'] = '-'
    balance_s_dict[key] = balance_s_dict[key].sort_index().reset_index(drop=True)
    balance_s_dict[key].loc[7.5,'Balance Sheet'] = 'Non-Current Assets'
    balance_s_dict[key] = balance_s_dict[key].sort_index().reset_index(drop=True)
    balance_s_dict[key].loc[11.5] = balance_s_dict[key].loc[9] + balance_s_dict[key].loc[10] + balance_s_dict[key].loc[11]
    balance_s_dict[key].loc[11.5,'Balance Sheet'] = 'Total Non-Current Asset'
    balance_s_dict[key] = balance_s_dict[key].sort_index().reset_index(drop=True)
    balance_s_dict[key].loc[12.5,'Balance Sheet'] = '-'
    balance_s_dict[key] = balance_s_dict[key].sort_index().reset_index(drop=True)
    balance_s_dict[key].loc[13.5,'Balance Sheet'] = 'Liabilities & Shareholders Equity'
    balance_s_dict[key] = balance_s_dict[key].sort_index().reset_index(drop=True)
    balance_s_dict[key].loc[14.5,'Balance Sheet'] = 'Current Liabilities'
    balance_s_dict[key] = balance_s_dict[key].sort_index().reset_index(drop=True)
    balance_s_dict[key].loc[22,'Balance Sheet'] = '08-Accounts Payable External'
    balance_s_dict[key].loc[19.5] = balance_s_dict[key].loc[16] + balance_s_dict[key].loc[17] + balance_s_dict[key].loc[18] + balance_s_dict[key].loc[19]
    balance_s_dict[key].loc[19.5,'Balance Sheet'] = 'Total Current Liabilities'
    balance_s_dict[key] = balance_s_dict[key].sort_index().reset_index(drop=True)
    balance_s_dict[key].loc[20.5] = balance_s_dict[key].loc[23] + balance_s_dict[key].loc[17] + balance_s_dict[key].loc[18] + balance_s_dict[key].loc[19]
    balance_s_dict[key].loc[20.5,'Balance Sheet'] = 'Total Current Liabilities*'
    balance_s_dict[key] = balance_s_dict[key].sort_index().reset_index(drop=True)
    balance_s_dict[key].loc[21.5,'Balance Sheet'] = '-'
    balance_s_dict[key] = balance_s_dict[key].sort_index().reset_index(drop=True)
    balance_s_dict[key].loc[22.5,'Balance Sheet'] = 'Total Non-Current Liabilities'
    balance_s_dict[key] = balance_s_dict[key].sort_index().reset_index(drop=True)
    balance_s_dict[key].loc[24.5,'Balance Sheet'] = '-'
    balance_s_dict[key] = balance_s_dict[key].sort_index().reset_index(drop=True).fillna(0).round(1)



# %%
income_s_dict.keys()
if 100004 in income_s_dict:
    del income_s_dict[100004]
income_s_dict.keys()

# %%


#cash flow statement
cashflow_s_dict = {}
for key in income_s_dict:
    df = income_s_dict[key].rename(columns={'Income Statement':'Description'})
    df_b = balance_s_dict[key].rename(columns={'Balance Sheet':'Description'})
    df_b.loc[28] = df_b.loc[6] + df_b.loc[12] + df_b.loc[20] + df_b.loc[24] + df_b.loc[26]
    df_b.loc[28,'Description'] = 'Net Balance'
    
    #create a temporary dataframe which caluclates the difference between the 2 years
    df_tmp = df_b.set_index('Description').diff(axis=1).reset_index().fillna('-')
    
    df2 = pd.DataFrame(columns=df_tmp.columns)
    
    df2.loc[0,'Description'] = 'Activities'
    entry = df.loc[df['Description']=='10-Net Income']
    df2 = df2.append([entry])
    df2.loc[2] = 0
    df2.loc[2,'Description'] = 'Depreciation and amortization'
    entry = df_tmp.select_dtypes(include=np.number).loc[(df_tmp['Description']=='02-Accounts Receivable') | (df_tmp['Description']=='03-Inventories') |  (df_tmp['Description']=='04-Prepaid Expenses')].sum()
    df2 = df2.append([entry]).reset_index(drop=True)
    df2.loc[3,'Description'] = 'Increase/Decrease in Current Asset'
    entry = df_tmp.select_dtypes(include=np.number).loc[(df_tmp['Description']=='08-Accounts Payable') | (df_tmp['Description']=='09-Accrued Liabilities') |  (df_tmp['Description']=='10-Other Short Term Liabilities')].sum()
    df2 = df2.append([entry]).reset_index(drop=True)
    df2.loc[4,'Description'] = 'Increase/Decrease in Current Liabilities'
    df2.loc[5] = 0
    df2.loc[5,'Description'] = 'Other operating cash flow adjustments'
    df2.loc[6,'Description'] = '-'
    df2.loc[7,'Description'] = 'Investing Activities'
    entry = df_tmp.select_dtypes(include=np.number).loc[(df_tmp['Description']=='Total Non-Current Asset')]
    df2 = df2.append([entry]).reset_index(drop=True)
    df2.loc[8,'Description'] = 'Capital asset acquisitions'
    df2.loc[9] = 0
    df2.loc[9,'Description'] = 'Capital asset disposal'
    df2.loc[10] = 0
    df2.loc[10,'Description'] = 'Other investing cash flows'
    df2.loc[11,'Description'] = 0
    df2.loc[12,'Description'] = 'Financing Activities'
    entry = df_tmp.loc[(df_tmp['Description']=='11-Debt')]
    df2 = df2.append([entry]).reset_index(drop=True)
    df2.loc[13,'Description'] = 'Increase/Decrease in Debt'
    entry = -df_tmp.select_dtypes(include=np.number).loc[(df_tmp['Description']=='13-Total Shareholders Equity')] + df_tmp.select_dtypes(include=np.number).loc[(df_tmp['Description']=='Net Balance')].sum()
    df2 = df2.append([entry]).reset_index(drop=True)
    df2.loc[14,'Description'] = 'Increase/Decrease in Equity'
    entry = -df_tmp.select_dtypes(include=np.number).loc[(df_tmp['Description']=='12-Other Long Term Liabilities')]
    df2 = df2.append([entry]).reset_index(drop=True)
    df2.loc[15,'Description'] = 'Other financing cash flows'
    df2.loc[16,'Description'] = 0
    entry = (-df2.select_dtypes(include=np.number).loc[(df2['Description']=='Increase/Decrease in Current Asset')].sum() - df2.select_dtypes(include=np.number).loc[(df2['Description']=='Increase/Decrease in Current Liabilities')].sum() - df2.select_dtypes(include=np.number).loc[(df2['Description']=='Capital asset acquisitions')].sum() - df2.select_dtypes(include=np.number).loc[(df2['Description']=='Increase/Decrease in Debt')].sum() + df2.select_dtypes(include=np.number).loc[(df2['Description']=='Increase/Decrease in Equity')].sum() + df2.select_dtypes(include=np.number).loc[(df2['Description']=='Other financing cash flows')].sum()).to_frame().transpose()
    df2 = df2.append([entry]).reset_index(drop=True)
    df2.loc[17,'Description'] = 'Change in Cash'
    entry = df_b[df_b['Description']=='01-Cash'].select_dtypes(include=np.number).shift(periods=1,axis='columns')
    df2 = df2.append([entry]).reset_index(drop=True)
    df2.loc[18,'Description'] = 'Year Beginning Cash'
    entry = df_b[df_b['Description']=='01-Cash']
    df2 = df2.append([entry]).reset_index(drop=True)
    df2.loc[19,'Description'] = 'Year Ending Cash'
    df2 = df2.round(1).fillna(0)

    cashflow_s_dict[key] = df2

# %%

statement_3_dict = {}
for key in income_s_dict:
    df = income_s_dict[key].rename(columns={'Income Statement':'Description'})
    df_b = balance_s_dict[key].rename(columns={'Balance Sheet':'Description'})
    df_c = cashflow_s_dict[key]
    
    df1 = pd.concat([df,df_b,df_c]).reset_index(drop=True)
    df1.loc[-0.5,'Description'] = 'Income Statement'
    df1 = df1.sort_index().reset_index(drop=True)
    df1.loc[13.5,'Description'] = '-'
    df1 = df1.sort_index().reset_index(drop=True)
    df1.loc[14.5,'Description'] = 'Balance Sheet'
    df1 = df1.sort_index().reset_index(drop=True)
    df1.loc[43.5,'Description'] = '-'
    df1 = df1.sort_index().reset_index(drop=True)
    df1.loc[44.5,'Description'] = 'Cashflow Statement'
    df1 = df1.sort_index().reset_index(drop=True).fillna('-').round(2)
    
    column_l = df1.columns.to_list()[1:]
    for x in column_l:
        df1[x] = pd.to_numeric(df1[x],errors='coerce')

    df2 = pd.DataFrame(columns = df.columns)
    df1 = df1.set_index('Description')
    
    print(key,df2.columns)
    
    days_in_p = 365
    df2.loc[0,'Description'] = 'Ratios'
    df2.loc[1,'Description'] = 'Income Statement'
    try:
        df2.loc[2] = df1.select_dtypes(include=np.number).loc['02-Cost of Revenue']*100/df1.select_dtypes(include=np.number).loc['01-Revenue']
    except:
        df2.loc[2]  = '-'
    df2.loc[2,'Description'] = 'COGS Ratio'
    try:
        df2.loc[3] = df1.select_dtypes(include=np.number).loc['Gross Profit']*100/df1.select_dtypes(include=np.number).loc['01-Revenue']
    except:
        df2.loc[3]  = '-'
    df2.loc[3,'Description'] = 'Gross Profit Ratio'
    try:
        df2.loc[4] = df1.select_dtypes(include=np.number).loc['EBIT']*100/df1.select_dtypes(include=np.number).loc['01-Revenue']
    except:
        df2.loc[4]  = '-'
    df2.loc[4,'Description'] = 'Operating Profit Ratio'
    try:
        df2.loc[5] = (df1.select_dtypes(include=np.number).loc['09-Income Tax & VAT']+df1.select_dtypes(include=np.number).loc['EBT'])*100/df1.select_dtypes(include=np.number).loc['01-Revenue']
    except:
        df2.loc[5] = '-'
    df2.loc[5,'Description'] = 'Net Profit Ratio'
    df2.loc[6,'Description'] = '-'
    try:
        df2.loc[7] = df1.select_dtypes(include=np.number).loc['09-Income Tax & VAT']/df1.select_dtypes(include=np.number).loc['EBT']
    except:
        df2.loc[7] = '-'
    df2.loc[7,'Description'] = 'Tax Ratio'
    try:
        df2.loc[8] = df1.select_dtypes(include=np.number).loc['08-Interest Expense']*-1/df1.select_dtypes(include=np.number).loc['EBIT']
    except:
        df2.loc[8]  = '-'
    df2.loc[8,'Description'] = 'Interest Coverage'
    df2.loc[9,'Description'] = '-'
    try:
        df2.loc[10] = df1.select_dtypes(include=np.number).loc['03-Office & Administrative Expenses']*100/df1.select_dtypes(include=np.number).loc['01-Revenue']
    except:
        df2.loc[10] = '-'
    df2.loc[10,'Description'] = 'OAE Ratio'
    try:
        df2.loc[11] = df1.select_dtypes(include=np.number).loc['04-Sales & Distribution Expenses']*100/df1.select_dtypes(include=np.number).loc['01-Revenue']
    except:
        df2.loc[11] = '-'
    df2.loc[11,'Description'] = 'Sales Ratio'
    try:
        df2.loc[12] = df1.select_dtypes(include=np.number).loc['06-Unusual Expenses (Income)']*100/df1.select_dtypes(include=np.number).loc['01-Revenue']
    except:
        df2.loc[12] = '-'
    df2.loc[12,'Description'] = 'Unusual Ratio'
    try:
        df2.loc[13] = df1.select_dtypes(include=np.number).loc['07-Other Operating Expenses, Total']*100/df1.select_dtypes(include=np.number).loc['01-Revenue']
    except:
        df2.loc[13] = '-'
    df2.loc[13,'Description'] = 'Other Expenses Ratio'
    try:
        df2.loc[14] = df1.select_dtypes(include=np.number).loc['08-Interest Expense']*100/df1.select_dtypes(include=np.number).loc['01-Revenue']
    except:
        df2.loc[14] = '-'
    df2.loc[14,'Description'] = 'Interest Ratio'
    df2.loc[15,'Description'] = '-'
    try:
        df2.loc[16] = (df1.select_dtypes(include=np.number).loc['Total Current Asset']-df1.select_dtypes(include=np.number).loc['03-Inventories'])*-1/df1.select_dtypes(include=np.number).loc['Total Current Liabilities']
    except:
        df2.loc[16] = '-'
    df2.loc[16,'Description'] = 'Quick Ratio'
    try:
        df2.loc[17] = (df1.select_dtypes(include=np.number).loc['Total Current Asset']-df1.select_dtypes(include=np.number).loc['03-Inventories'])*-1/df1.select_dtypes(include=np.number).loc['Total Current Liabilities*']
    except:
        df2.loc[17] = '-'
    df2.loc[17,'Description'] = 'Quick Ratio Adjusted'
    try:
        df2.loc[18] = df1.select_dtypes(include=np.number).loc['Total Current Asset']*-1/df1.select_dtypes(include=np.number).loc['Total Current Liabilities']
    except:
        df2.loc[18] = '-'
    df2.loc[18,'Description'] = 'Current Ratio'
    try:
        df2.loc[19] = (df1.select_dtypes(include=np.number).loc['Total Current Asset'])*-1/df1.select_dtypes(include=np.number).loc['Total Current Liabilities*']
    except:
        df2.loc[19] = '-'
    df2.loc[19,'Description'] = 'Current Ratio Adjusted'
    try:
        df2.loc[20] = df1.select_dtypes(include=np.number).loc['01-Revenue']*-1/(df1.select_dtypes(include=np.number).loc['Total Current Asset']+df1.select_dtypes(include=np.number).loc['Total Non-Current Asset'])
    except:
        df2.loc[20] = '-'
    df2.loc[20,'Description'] = 'Total Asset Turnover Ratio'
    try:
        df2.loc[21] = df1.select_dtypes(include=np.number).loc['01-Revenue']*-1/(df1.select_dtypes(include=np.number).loc['Total Current Asset']+df1.select_dtypes(include=np.number).loc['Total Non-Current Asset']+df1.select_dtypes(include=np.number).loc['Total Current Liabilities']+df1.select_dtypes(include=np.number).loc['12-Other Long Term Liabilities'])
    except:
        df2.loc[21] = '-'
    df2.loc[21,'Description'] = 'Net Asset Turnover Ratio'
    try:
        df2.loc[22] = df1.select_dtypes(include=np.number).loc['01-Revenue']*-1/(df1.select_dtypes(include=np.number).loc['Total Current Asset']+df1.select_dtypes(include=np.number).loc['Total Non-Current Asset']+df1.select_dtypes(include=np.number).loc['Total Current Liabilities*']+df1.select_dtypes(include=np.number).loc['12-Other Long Term Liabilities'])
    except:
        df2.loc[22] = '-'
    df2.loc[22,'Description'] = 'Net Asset Turnover Ratio Adjusted'
    df2.loc[23,'Description'] = '-'
    try:
        df2.loc[24] = df1.select_dtypes(include=np.number).loc['02-Cost of Revenue']/df1.select_dtypes(include=np.number).loc['03-Inventories']
    except:
        df2.loc[24] = '-'
    df2.loc[24,'Description'] = 'Inventory Turnover'
    try:
        df2.loc[25] = df1.select_dtypes(include=np.number).loc['03-Inventories']*days_in_p/df1.select_dtypes(include=np.number).loc['02-Cost of Revenue']
    except:
        df2.loc[25] = '-'
    df2.loc[25,'Description'] = 'Inventory Days'
    try:
        df2.loc[26] = df1.select_dtypes(include=np.number).loc['01-Revenue']*-1/df1.select_dtypes(include=np.number).loc['02-Accounts Receivable']
    except:
        df2.loc[26] = '-'
    df2.loc[26,'Description'] = 'Accounts Receivable Turnover'
    try:
        df2.loc[27] = df1.select_dtypes(include=np.number).loc['02-Accounts Receivable']*-days_in_p/df1.select_dtypes(include=np.number).loc['01-Revenue']
    except:
        df2.loc[27] = '-'
    df2.loc[27,'Description'] = 'A/R Days'
    try:
        df2.loc[28] = df1.select_dtypes(include=np.number).loc['01-Revenue']*-1/df1.select_dtypes(include=np.number).loc['02-Accounts Receivable']
    except:
        df2.loc[28] = '-'
    df2.loc[28,'Description'] = 'Accounts Receivable Turnover'
    try:
        df2.loc[29] = df1.select_dtypes(include=np.number).loc['02-Accounts Receivable']*-days_in_p/df1.select_dtypes(include=np.number).loc['01-Revenue']
    except:
        df2.loc[29] = '-'
    df2.loc[29,'Description'] = 'A/R Days'
    try:
        df2.loc[30] = df1.select_dtypes(include=np.number).loc['02-Cost of Revenue']*-1/df1.select_dtypes(include=np.number).loc['08-Accounts Payable']
    except:
        df2.loc[30] = '-'
    df2.loc[30,'Description'] = 'Accounts Payable Turnover'
    try:
        df2.loc[31] = df1.select_dtypes(include=np.number).loc['08-Accounts Payable']*-days_in_p/df1.select_dtypes(include=np.number).loc['02-Cost of Revenue']
    except:
        df2.loc[31] = '-'
    df2.loc[31,'Description'] = 'A/P Days'
    try:
        df2.loc[32] = df1.select_dtypes(include=np.number).loc['02-Cost of Revenue']*-1/df1.select_dtypes(include=np.number).loc['08-Accounts Payable External']
    except:
        df2.loc[32] = '-'
    df2.loc[32,'Description'] = 'Accounts Payable Turnover Adjusted'
    try:
        df2.loc[33] = df1.select_dtypes(include=np.number).loc['08-Accounts Payable External']*-days_in_p/df1.select_dtypes(include=np.number).loc['02-Cost of Revenue']
    except:
        df2.loc[33] = '-'
    df2.loc[33,'Description'] = 'A/P Days Adjusted'
    try:
        df2.loc[34] = df1.select_dtypes(include=np.number).loc['06-Property, Plant & Equipment']*-1/df1.select_dtypes(include=np.number).loc['01-Revenue']
    except:
        df2.loc[34] = '-'
    df2.loc[34,'Description'] = 'PP&E Ratio'
    try:
        df2.loc[35] = df1.select_dtypes(include=np.number).loc['01-Revenue']*-1/(df1.select_dtypes(include=np.number).loc['02-Accounts Receivable']+df1.select_dtypes(include=np.number).loc['03-Inventories']+df1.select_dtypes(include=np.number).loc['08-Accounts Payable'])
    except:
        df2.loc[35] = '-'
    df2.loc[35,'Description'] = 'Working Capital Turnover'
    try:
        df2.loc[36] = df1.select_dtypes(include=np.number).loc['01-Revenue']*-1/(df1.select_dtypes(include=np.number).loc['02-Accounts Receivable']+df1.select_dtypes(include=np.number).loc['03-Inventories']+df1.select_dtypes(include=np.number).loc['08-Accounts Payable External'])
    except:
        df2.loc[36] = '-'
    df2.loc[36,'Description'] = 'Working Capital Turnover Adjusted'
    try:
        df2.loc[37] = df1.select_dtypes(include=np.number).loc['01-Revenue']*-1/df1.select_dtypes(include=np.number).loc['01-Cash']
    except:
        df2.loc[37] = '-'
    df2.loc[37,'Description'] = 'Cash Turnover'
    df2.loc[38,'Description'] = '-'
    try:
        df2.loc[39] = df1.select_dtypes(include=np.number).loc['11-Debt']/df1.select_dtypes(include=np.number).loc['13-Total Shareholders Equity']
    except:
        df2.loc[39] = '-'
    df2.loc[39,'Description'] = 'Debt/Equity'
    try:
        df2.loc[40] = df1.select_dtypes(include=np.number).loc['11-Debt']/(df1.select_dtypes(include=np.number).loc['13-Total Shareholders Equity']-df1.select_dtypes(include=np.number).loc['11-Debt'])
    except:
        df2.loc[40] = '-'
    df2.loc[40,'Description'] = 'Debt/Capital'
    try:
        df2.loc[41] = df1.select_dtypes(include=np.number).loc['11-Debt']*-1/(df1.select_dtypes(include=np.number).loc['Total Non-Current Asset']-df1.select_dtypes(include=np.number).loc['07-Goodwill & Intangible Asset'])
    except:
        df2.loc[41] = '-'
    df2.loc[41,'Description'] = 'Debt to Tangible Net Worth'
    try:
        df2.loc[42] = (df1.select_dtypes(include=np.number).loc['Total Current Liabilities']+df1.select_dtypes(include=np.number).loc['12-Other Long Term Liabilities'])/df1.select_dtypes(include=np.number).loc['13-Total Shareholders Equity']
    except:
        df2.loc[42] = '-'
    df2.loc[42,'Description'] = 'Total Liabilities to Equity'
    try:
        df2.loc[43] = (df1.select_dtypes(include=np.number).loc['Total Current Liabilities*']+df1.select_dtypes(include=np.number).loc['12-Other Long Term Liabilities'])/df1.select_dtypes(include=np.number).loc['13-Total Shareholders Equity']
    except:
        df2.loc[43] = '-'
    df2.loc[43,'Description'] = 'Total Liabilities to Equity Adjusted'
    try:
        df2.loc[44] = (df1.select_dtypes(include=np.number).loc['Total Current Asset']+df1.select_dtypes(include=np.number).loc['Total Non-Current Asset'])/df1.select_dtypes(include=np.number).loc['13-Total Shareholders Equity']
    except:
        df2.loc[44] = '-'
    df2.loc[44,'Description'] = 'Total Assets to Equity'
    try:
        df2.loc[45] = df1.select_dtypes(include=np.number).loc['11-Debt']/(df1.select_dtypes(include=np.number).loc['EBIT']-df1.select_dtypes(include=np.number).loc['05-Depreciation/Amortization'])
    except:
        df2.loc[45] = '-'
    df2.loc[45,'Description'] = 'Debt/EBITDA'
    try:
        df2.loc[46] = df1.select_dtypes(include=np.number).loc['EBT']*-1/df1.select_dtypes(include=np.number).loc['EBIT']
    except:
        df2.loc[46] = '-'
    df2.loc[46,'Description'] = 'Capital Structure Impact'
    try:
        df2.loc[47] = (df1.select_dtypes(include=np.number).loc['Total Current Asset']-df1.select_dtypes(include=np.number).loc['03-Inventories'])*-1/df1.select_dtypes(include=np.number).loc['Total Current Liabilities']
    except:
        df2.loc[47] = '-'
    df2.loc[47,'Description'] = 'Acid Test'
    try:
        df2.loc[48] = (df1.select_dtypes(include=np.number).loc['Total Current Asset']-df1.select_dtypes(include=np.number).loc['03-Inventories'])*-1/df1.select_dtypes(include=np.number).loc['Total Current Liabilities*']
    except:
        df2.loc[48] = '-'
    df2.loc[48,'Description'] = 'Acid Test Adjusted'
    df2.loc[49,'Description'] = '-'
    try:
        df2.loc[50] = (df1.select_dtypes(include=np.number).loc['09-Income Tax & VAT']+df1.select_dtypes(include=np.number).loc['EBT'])/df1.select_dtypes(include=np.number).loc['13-Total Shareholders Equity']*-1
    except:
        df2.loc[50] = '-'
    df2.loc[50,'Description'] = 'Return on Equity'
    try:
        df2.loc[51] = (df1.select_dtypes(include=np.number).loc['09-Income Tax & VAT']+df1.select_dtypes(include=np.number).loc['EBT'])/(df1.select_dtypes(include=np.number).loc['Total Current Asset']+df1.select_dtypes(include=np.number).loc['Total Non-Current Asset'])
    except:
        df2.loc[51] = '-'
    df2.loc[51,'Description'] = 'Return on Assets'
    df2 = df2.fillna('-')
    
    df1 = df1.reset_index()
    
    df1 = pd.concat([df1,df2]).reset_index(drop=True)
    
    statement_3_dict[key] = df1

# %%
main_data_dict_pl

# %%

zid_dict = {100000:'GI_Corp',100001:'Trading',100005:'Zepto',100006:'Grocery',100009:'Packaging'}

# take income of Trading, GI_Corp, Zepto & Grocery for the 3 years in 3 different dataframes

pl_data_income = main_data_dict_pl
income_dict = {}
for key in pl_data_income:
    df = pl_data_income[key]
    for i in year_list:
        income_dict[key] = [df[df['xacctype'] == 'Income'].sum()[i] for i in year_list]
income_df = pd.DataFrame.from_dict(income_dict,orient = 'index', columns = [i for i in year_list]).reset_index()
income_df['Name'] = income_df['index'].map(zid_dict)
new_cols = ['index','Name']+[i for i in year_list] 
income_df = income_df[new_cols]
income_df.loc[len(income_df.index),:] = income_df.sum(axis=0,numeric_only=True)
income_df.at[len(income_df.index)-1,'Name'] = 'Total'

pl_data_COGS = main_data_dict_pl
COGS_dict = {}

# %%
pl_data_COGS

# %%
if 100004 in pl_data_COGS:
    del pl_data_COGS[100004]
pl_data_COGS.keys()

# %%

# Process COGS data
for key in pl_data_COGS:
    df = pl_data_COGS[key]
    if key != 100000:
        if not df[df['xacc'] == '04010020'].empty:
            COGS_dict[key] = [
                df[df['xacc'] == '04010020'][i][df.loc[df['xacc'] == '04010020'].index[0]]
                for i in year_list
            ]
        else:
            COGS_dict[key] = [0] * len(year_list)  # Default value if no matching rows
    else:
        if not df[df['xacc'] == '4010020'].empty:
            COGS_dict[key] = [
                df[df['xacc'] == '4010020'][i][df.loc[df['xacc'] == '4010020'].index[0]]
                for i in year_list
            ]
        else:
            COGS_dict[key] = [0] * len(year_list)  # Default value if no matching rows

# Create COGS DataFrame
COGS_df = pd.DataFrame.from_dict(COGS_dict, orient='index', columns=[i for i in year_list]).reset_index()
COGS_df['Name'] = COGS_df['index'].map(zid_dict)
new_cols = ['index', 'Name'] + [i for i in year_list]
COGS_df = COGS_df[new_cols]
COGS_df.loc[len(COGS_df.index), :] = COGS_df.sum(axis=0, numeric_only=True)
COGS_df.at[len(COGS_df.index) - 1, 'Name'] = 'Total'

# Process Expense data
pl_data_expense = main_data_dict_pl  # Replace with your actual expense data dictionary
expense_dict = {}

for key in pl_data_expense:
    df = pl_data_expense[key]
    if not df[(df['xacc'] != '04010020') & (df['xacctype'] == 'Expenditure')].empty:
        expense_dict[key] = [
            df[(df['xacc'] != '04010020') & (df['xacctype'] == 'Expenditure')].sum()[i]
            for i in year_list
        ]
    else:
        expense_dict[key] = [0] * len(year_list)  # Default value if no matching rows

# Create Expense DataFrame
expense_df = pd.DataFrame.from_dict(expense_dict, orient='index', columns=[i for i in year_list]).reset_index()
expense_df['Name'] = expense_df['index'].map(zid_dict)
new_cols = ['index', 'Name'] + [i for i in year_list]
expense_df = expense_df[new_cols]
expense_df.loc[len(expense_df.index), :] = expense_df.sum(axis=0, numeric_only=True)
expense_df.at[len(expense_df.index) - 1, 'Name'] = 'Total'

# Display final DataFrames
print("COGS DataFrame:")
print(COGS_df)
print("\nExpense DataFrame:")
print(expense_df)

# %%

pl_data_profit = main_data_dict_pl
profit_dict = {}
for key in pl_data_profit:
    df = pl_data_profit[key]
    for i in year_list:
        profit_dict[key] = [df.sum()[i] for i in year_list]
profit_df = pd.DataFrame.from_dict(profit_dict,orient = 'index', columns = [i for i in year_list]).reset_index()
profit_df['Name'] = profit_df['index'].map(zid_dict)
new_cols = ['index','Name']+[i for i in year_list] 
profit_df = profit_df[new_cols]
profit_df.loc[len(profit_df.index),:] = profit_df.sum(axis=0,numeric_only=True)
profit_df.at[len(profit_df.index)-1,'Name'] = 'Total'

## taxes should be separated according to VAT and income tax. Also I think now the structure is even more different
pl_data_EBITDA = level_3_dict
EBITDA_dict = {}
for key in pl_data_EBITDA:
    df = pl_data_EBITDA[key]
    for i in year_list:
        EBITDA_dict[key] = [df[(df['xhrc2']!='0625-Property Tax & Others') & (df['xhrc2']!='0604-City Corporation Tax') & (df['xhrc2']!='0629- HMBR VAT & Tax Expenses') & (df['xhrc2']!='0630- Bank Interest & Charges') & (df['xhrc2']!='0633-Interest-Loan') & (df['xhrc2']!='0636-Depreciation') & (df['xhrc2']!='Profit/Loss')].sum()[i] for i in year_list]
EBITDA_df = pd.DataFrame.from_dict(EBITDA_dict,orient = 'index', columns = [i for i in year_list]).reset_index()
EBITDA_df['Name'] = EBITDA_df['index'].map(zid_dict)
new_cols = ['index','Name']+[i for i in year_list] 
EBITDA_df = EBITDA_df[new_cols]
EBITDA_df.loc[len(EBITDA_df.index),:] = EBITDA_df.sum(axis=0,numeric_only=True)
EBITDA_df.at[len(EBITDA_df.index)-1,'Name'] = 'Total'

# %%

pl_data_tax = level_3_dict
tax_dict = {}
for key in pl_data_tax:
    df = pl_data_tax[key]
    for i in year_list:
        tax_dict[key] = [df[(df['xhrc2']=='0625-Property Tax & Others') | (df['xhrc2']=='0604-City Corporation Tax') | (df['xhrc2']=='0629- HMBR VAT & Tax Expenses')].sum()[i] for i in year_list]
tax_df = pd.DataFrame.from_dict(tax_dict,orient = 'index', columns = [i for i in year_list]).reset_index()
tax_df['Name'] = tax_df['index'].map(zid_dict)
new_cols = ['index','Name']+[i for i in year_list] 
tax_df = tax_df[new_cols]
tax_df.loc[len(tax_df.index),:] = tax_df.sum(axis=0,numeric_only=True)
tax_df.at[len(tax_df.index)-1,'Name'] = 'Total'

pl_data_interest = level_3_dict
interest_dict = {}
for key in pl_data_interest:
    df = pl_data_interest[key]
    for i in year_list:
        interest_dict[key] = [df[(df['xhrc2']=='0630- Bank Interest & Charges') | (df['xhrc2']=='0633-Interest-Loan')].sum()[i] for i in year_list] ### here 
interest_df = pd.DataFrame.from_dict(interest_dict,orient = 'index', columns = [i for i in year_list]).reset_index()
interest_df['Name'] = interest_df['index'].map(zid_dict)
new_cols = ['index','Name']+[i for i in year_list] 
interest_df = interest_df[new_cols]
interest_df.loc[len(interest_df.index),:] = interest_df.sum(axis=0,numeric_only=True)
interest_df.at[len(interest_df.index)-1,'Name'] = 'Total'


# %%

##New code addition by director on 19112022 regarding ap ar and inv
pl_data_apari = main_data_dict_bs
apari_dict = {}
for key in pl_data_apari:
    if key != 100000:
        df = pl_data_apari[key]
        apari_dict[key] = df[(df['xacc'] == '09030001')|(df['xacc'] == '01030001')|(df['xacc'] == '01060003')|(df['xacc'] == '01060001')]
        apari_dict[key]['Business'] = key
    apari_df = pd.concat([apari_dict[key] for key in apari_dict],axis=0)
    apari_df['Name'] = apari_df['Business'].map(zid_dict)


# %%

###Profit & loss
hmbr_pl = main_data_dict_pl[100001]
GI_Corp_pl = main_data_dict_pl[100000]
zepto_pl = main_data_dict_pl[100005]
grocery_pl = main_data_dict_pl[100006]
packaging_pl = main_data_dict_pl[100009]

# %%

hmbr_bs = main_data_dict_bs[100001]
GI_Corp_bs = main_data_dict_bs[100000]
zepto_bs = main_data_dict_bs[100005]
grocery_bs = main_data_dict_bs[100006]
packaging_bs = main_data_dict_bs[100009]

# %%

### all balance sheet together
all_bs = pd.concat(main_data_dict_bs,axis=0)

### Summery Details
hmbr_summery = level_1_dict[100001]
GI_Corp_summery = level_1_dict[100000]
zepto_summery = level_1_dict[100005]
grocery_summery = level_1_dict[100006]
packaging_summery = level_1_dict[100009]


# %%

##lvl 4
hmbr_summery_lvl_4 = level_4_dict[100001]
GI_Corp_summery_lvl_4 = level_4_dict[100000]
zepto_summery_lvl_4 = level_4_dict[100005]
grocery_summery_lvl_4 = level_4_dict[100006]
packaging_summery_lvl_4 = level_4_dict[100009]

all_lvl_4 = pd.concat(level_4_dict,axis = 0)


# %%
level_4_dict_bs.keys()

# %%

hmbr_summery_lvl_4_bs = level_4_dict_bs[100001]
GI_Corp_summery_lvl_4_bs = level_4_dict_bs[100000]
zepto_summery_lvl_4_bs = level_4_dict_bs[100005]
grocery_summery_lvl_4_bs = level_4_dict_bs[100006]
packaging_summery_lvl_4_bs = level_4_dict_bs[100009]

all_lvl_4_bs = pd.concat(level_4_dict_bs,axis = 0)

hmbr_summery_ap_final_dict = ap_final_dict[100001]
GI_Corp_summery_ap_final_dict = ap_final_dict[100000]
zepto_summery_ap_final_dict = ap_final_dict[100005]
grocery_summery_ap_final_dict = ap_final_dict[100006]
packaging_summery_ap_final_dict = ap_final_dict[100009]

all_ap_final_dict = pd.concat(ap_final_dict,axis=0)

hmbr_summery_statements = statement_3_dict[100001]
GI_Corp_summery_statements = statement_3_dict[100000]
zepto_summery_statements = statement_3_dict[100005]
grocery_summery_statements = statement_3_dict[100006]
packaging_summery_statements = statement_3_dict[100009]

# %%

with pd.ExcelWriter('level_4.xlsx') as writer:  
    hmbr_summery_lvl_4.to_excel(writer, sheet_name='100001')
    GI_Corp_summery_lvl_4.to_excel(writer, sheet_name='100000')
    zepto_summery_lvl_4.to_excel(writer, sheet_name='100005')
    grocery_summery_lvl_4.to_excel(writer, sheet_name='100006')
    packaging_summery_lvl_4.to_excel(writer, sheet_name='100009')

###Excel File Generate
profit_excel = f'p&l{start_year}_{start_month}_{end_month}.xlsx'
balance_excel = f'b&l{start_year}_{start_month}_{end_month}.xlsx'
details_excel = f'profitLossDetail{start_year}_{start_month}_{end_month}.xlsx'
lvl_4_details_excel = f'level_4{start_year}_{start_month}_{end_month}.xlsx'
lvl_4_bs_details_excel = f'level_4_bs{start_year}_{start_month}_{end_month}.xlsx'
ap_final_dict_excel = f'ap_final_dict{start_year}_{start_month}_{end_month}.xlsx'
statement_3_dict_excel = f'statement_3_dict{start_year}_{start_month}_{end_month}.xlsx'
with pd.ExcelWriter(profit_excel) as writer:  
    hmbr_pl.to_excel(writer, sheet_name='100001')
    GI_Corp_pl.to_excel(writer, sheet_name='100000')
    zepto_pl.to_excel(writer, sheet_name='100005')
    grocery_pl.to_excel(writer, sheet_name='100006')
    packaging_pl.to_excel(writer, sheet_name='100009')

with pd.ExcelWriter(balance_excel) as writer:  
    hmbr_bs.to_excel(writer, sheet_name='100001')
    GI_Corp_bs.to_excel(writer, sheet_name='100000')
    zepto_bs.to_excel(writer, sheet_name='100005')
    grocery_bs.to_excel(writer, sheet_name='100006')
    packaging_bs.to_excel(writer, sheet_name='100009')
    all_bs.to_excel(writer, sheet_name='all_bs')

# income_df COGS_df expense_df, profit_df asset_df liable_df
with pd.ExcelWriter(details_excel) as writer:  
    income_df.to_excel(writer, sheet_name='income')
    COGS_df.to_excel(writer, sheet_name='COGS')
    expense_df.to_excel(writer, sheet_name='expense')
    profit_df.to_excel(writer, sheet_name='profit')
    # asset_df.to_excel(writer, sheet_name='asset')
    # liable_df.to_excel(writer, sheet_name='liable')
    apari_df.to_excel(writer,sheet_name='apari')
    EBITDA_df.to_excel(writer,sheet_name='EBITDA')
    interest_df.to_excel(writer,sheet_name='interest')
    tax_df.to_excel(writer,sheet_name='tax')
# income_df COGS_df expense_df, profit_df asset_df liable_df
#lvl-4
with pd.ExcelWriter(lvl_4_details_excel) as writer:  
    hmbr_summery_lvl_4.to_excel(writer, sheet_name='100001')
    GI_Corp_summery_lvl_4.to_excel(writer, sheet_name='100000')
    zepto_summery_lvl_4.to_excel(writer, sheet_name='100005')
    grocery_summery_lvl_4.to_excel(writer, sheet_name='100006')
    packaging_summery_lvl_4.to_excel(writer, sheet_name='100009')
    all_lvl_4.to_excel(writer,sheet_name='all_lvl_4')
#lvl4-bs
with pd.ExcelWriter(lvl_4_bs_details_excel) as writer:  
    hmbr_summery_lvl_4_bs.to_excel(writer, sheet_name='100001')
    GI_Corp_summery_lvl_4_bs.to_excel(writer, sheet_name='100000')
    zepto_summery_lvl_4_bs.to_excel(writer, sheet_name='100005')
    grocery_summery_lvl_4_bs.to_excel(writer, sheet_name='100006')
    packaging_summery_lvl_4_bs.to_excel(writer, sheet_name='100009')
    all_lvl_4_bs.to_excel(writer,sheet_name='all_lvl_4_bs')

with pd.ExcelWriter(ap_final_dict_excel) as writer:
    hmbr_summery_ap_final_dict.to_excel(writer,sheet_name='100001')
    GI_Corp_summery_ap_final_dict.to_excel(writer,sheet_name='100000')
    zepto_summery_ap_final_dict.to_excel(writer,sheet_name='100005')
    grocery_summery_ap_final_dict.to_excel(writer,sheet_name='100006')
    packaging_summery_ap_final_dict.to_excel(writer,sheet_name='100009')
    all_ap_final_dict.to_excel(writer,sheet_name='all_ap_final_dict')

with pd.ExcelWriter(statement_3_dict_excel) as writer:
    hmbr_summery_statements.to_excel(writer,sheet_name='100001')
    GI_Corp_summery_statements.to_excel(writer,sheet_name='100000')
    zepto_summery_statements.to_excel(writer,sheet_name='100005')
    grocery_summery_statements.to_excel(writer,sheet_name='100006')
    packaging_summery_statements.to_excel(writer,sheet_name='100009')

# %%

# ###Email    
me = "pythonhmbr12@gmail.com"
you = ["ithmbrbd@gmail.com", "asaddat87@gmail.com", "motiurhmbr@gmail.com", "hmbr12@gmail.com", ]
#you = ["ithmbrbd@gmail.com","admhmbr@gmail.com"]

msg = MIMEMultipart('alternative')
msg['Subject'] = f"profit & loss HMBR .year: {start_year} month from {start_month} to {end_month}"
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
# income_df COGS_df expense_df, profit_df asset_df liable_df
with open('profitLoss.html','w') as f:
    f.write(HEADER)
    f.write('HMBR Details')
    f.write(hmbr_summery.to_html(classes='df_summery'))
    f.write('GI Details')
    f.write(GI_Corp_summery.to_html(classes='df_summery1'))
    f.write(zepto_summery.to_html(classes='df_summery5'))
    f.write('Grocery Details')
    f.write(grocery_summery.to_html(classes='df_summery6'))
    f.write('Packaging Details')
    f.write(packaging_summery.to_html(classes='df_summery9'))
    f.write('Cost of good sold details')
    f.write(COGS_df.to_html(classes='df_summery10'))
    f.write('Income Details')
    f.write(income_df.to_html(classes='df_summery11'))
    f.write('Expense details')
    f.write(expense_df.to_html(classes='df_summery12'))
    f.write('Profit Details')
    f.write(profit_df.to_html(classes='df_summery13'))
    # f.write('Asset Details')
    # f.write(asset_df.to_html(classes='df_summery14'))
    # f.write('Liability Details')
    # f.write(liable_df.to_html(classes='df_summery15'))
    f.write(FOOTER)

filename = "profitLoss.html"
f = open(filename)
attachment = MIMEText(f.read(),'html')
msg.attach(attachment)

part1 = MIMEBase('application', "octet-stream")
part1.set_payload(open(profit_excel, "rb").read())
encoders.encode_base64(part1)
part1.add_header('Content-Disposition', 'attachment; filename="profit.xlsx"')
msg.attach(part1)

part2 = MIMEBase('application', "octet-stream")
part2.set_payload(open(balance_excel, "rb").read())
encoders.encode_base64(part2)
part2.add_header('Content-Disposition', 'attachment; filename="balance.xlsx"')
msg.attach(part2)

part3 = MIMEBase('application', "octet-stream")
part3.set_payload(open(details_excel, "rb").read())
encoders.encode_base64(part3)
part3.add_header('Content-Disposition', 'attachment; filename="profitLossDetail.xlsx"')
msg.attach(part3)

part4 = MIMEBase('application', "octet-stream")
part4.set_payload(open(lvl_4_details_excel, "rb").read())
encoders.encode_base64(part4)
part4.add_header('Content-Disposition', 'attachment; filename="lvl_3.xlsx"')
msg.attach(part4)

part5 = MIMEBase('application', "octet-stream")
part5.set_payload(open(lvl_4_bs_details_excel, "rb").read())
encoders.encode_base64(part5)
part5.add_header('Content-Disposition', 'attachment; filename="lvl_3bs_.xlsx"')
msg.attach(part5)

part6 = MIMEBase('application', "octet-stream")
part6.set_payload(open(ap_final_dict_excel, "rb").read())
encoders.encode_base64(part6)
part6.add_header('Content-Disposition', 'attachment; filename="ap_final_dict_.xlsx"')
msg.attach(part6)

part7 = MIMEBase('application', "octet-stream")
part7.set_payload(open(statement_3_dict_excel, "rb").read())
encoders.encode_base64(part7)
part7.add_header('Content-Disposition', 'attachment; filename="statement_3_dict_.xlsx"')
msg.attach(part7)

username = 'pythonhmbr12@gmail.com'
password = 'vksikttussvnbqef'


s = smtplib.SMTP('smtp.gmail.com:587')
s.starttls()
s.login(username, password)
s.sendmail(me,you,msg.as_string())
s.quit()


# %%



