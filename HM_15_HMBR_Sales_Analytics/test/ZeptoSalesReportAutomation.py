from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from datetime import date,datetime,timedelta
import psycopg2
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

def create_prmst(zid):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    df_prmst = pd.read_sql("select xemp,xname from prmst where zid = '%s'" % (zid), con = engine)
    return df_prmst

def create_cacus(zid):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    df_cacus = pd.read_sql("select zid,xcus,xshort,xadd2,xcity from cacus where zid = '%s'" % (zid), con = engine)
    return df_cacus

def create_caitem(zid):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    df_caitem = pd.read_sql("select zid,xitem,xdesc,xgitem,xstdprice,xsrate from caitem where zid = '%s'" % (zid), con = engine)
    return df_caitem

def create_opord(zid):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    df_opord = pd.read_sql("select xordernum,xdate,xcus,xdiv,xsp,xtotamt from opord where zid = '%s'" % (zid), con = engine)
    return df_opord

def create_opodt(zid):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    df_opodt = pd.read_sql("select zid,xordernum,xitem,xqtydel,xrate,xdisc,xdiscf,xlineamt,xdtwotax,xdtdisc,xdtcomm from opodt where zid = '%s'" % (zid), con = engine)
    return df_opodt

def create_opcrn(zid):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    df_opcrn = pd.read_sql("select xcrnnum,xdate,xcus,xdisc,xdiscf,xglref,xordernum,xemp from opcrn where zid = '%s'" % (zid), con = engine)
    return df_opcrn

def create_opcdt(zid):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    df_opcdt = pd.read_sql("select zid,xcrnnum,xitem,xqty,xdornum,xrate,xlineamt from opcdt where zid = '%s'" % (zid), con = engine)
    return df_opcdt

def create_rectreturn(zid):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    df_imtemptrn = pd.read_sql("select zid,ximtmptrn,xdate,xyear,xper,xcus,xemp,xarea,xtrnimf from imtemptrn where xstatustrn ='5-Confirmed' and zid = '%s'" % (zid), con = engine)
    df_imtemptdt = pd.read_sql("select ximtmptrn,xitem,xqtyord,xrate,xlineamt from imtemptdt where zid = '%s'" % (zid), con = engine)
    df_imtemptdt = df_imtemptdt.merge(df_imtemptrn,on='ximtmptrn',how='left')
    df_imtemptdt = df_imtemptdt[df_imtemptdt['xtrnimf']=='RECT']
    thisYear = datetime.now().year
    df_imtemptdt = df_imtemptdt[df_imtemptdt['xyear']==thisYear]
    return df_imtemptdt

def create_mainsheet(zid):
    df_prmst = create_prmst(zid)
    df_cacus = create_cacus(zid)
    df_caitem = create_caitem(zid)
    df_opord = create_opord(zid)
    df_opodt = create_opodt(zid)
    df_opcdt = create_opcdt(zid)
    df_opcrn = create_opcrn(zid)
    df_main_sale = df_opodt.merge(df_opord,on='xordernum',how='left')
    df_main_return = df_opcdt.merge(df_opcrn,on='xcrnnum',how='left')
    df_main_return = df_main_return.rename(columns={'zid':'zidreturn','xrate':'xratereturn','xlineamt':'xlineamtreturn','xdisc':'xdiscreturn','xdiscf':'xdiscfreturn','xcus':'xcusreturn','xdate':'xdatereturn'})
    df_main = df_main_sale.merge(df_main_return,on=['xordernum','xitem'],how='left')
    df_main = df_main.merge(df_cacus,on='xcus',how='left')
    df_main = df_main.merge(df_caitem,on='xitem', how='left')
    df_prmst = df_prmst.rename(columns={'xemp':'xsp'})
    df_main = df_main.merge(df_prmst,on='xsp',how='left')
    df_main = df_main.fillna(value=0,axis=1)
    df_main['xfinallineamt']= df_main['xlineamt']-df_main['xlineamtreturn']
    df_main['xfinalqtydel']= df_main['xqtydel']-df_main['xqty']
    df_main['xfinalrate'] = df_main['xfinallineamt']/df_main['xfinalqtydel']
    df_main = df_main.drop(['zid_x','zid_y'],axis=1)
    thisYear = datetime.now().year
    df_main['xdate'] = pd.to_datetime(df_main['xdate'])
    df_main['Year'] = df_main['xdate'].dt.year
    df_main['Month'] = df_main['xdate'].dt.month
    df_main = df_main[df_main['Year']==thisYear]
    return df_main


start_time = time.time()
zid = '100005'
df_main = create_mainsheet(zid)

#salesman wise product sales
df_salesman_product = df_main.groupby(['xsp','xname','xitem','xdesc']).sum()[['xfinalqtydel']]
df_salesman_product = df_salesman_product.reset_index()
df_salesman_product = df_salesman_product[df_salesman_product['xname']!=0]
df_salesman_product['spname'] = df_salesman_product['xsp'] + ':-' + df_salesman_product['xname']
df_salesman_product['itemdesc'] = df_salesman_product['xitem'] + ':-' + df_salesman_product['xdesc']
df_salesman_product = df_salesman_product.pivot(index='spname',columns='itemdesc',values='xfinalqtydel')
df_salesman_product = df_salesman_product.reset_index()
df_salesman_product = df_salesman_product.rename(columns={'spname':'Salesman Name'})
df_salesman_product.loc['sum'] = df_salesman_product.sum(axis=0)
df_salesman_product = df_salesman_product.fillna(value=0,axis=1)
df_salesman_product.loc['sum','Salesman Name'] = 0

thisMonth = datetime.now().month
df_main_month = df_main[df_main['Month']==thisMonth]
df_salesman_product_month = df_main_month.groupby(['xsp','xname','xitem','xdesc']).sum()[['xfinalqtydel']]
df_salesman_product_month = df_salesman_product_month.reset_index()
df_salesman_product_month = df_salesman_product_month[df_salesman_product_month['xname']!=0]
df_salesman_product_month['spname'] = df_salesman_product_month['xsp'] + ':-' + df_salesman_product_month['xname']
df_salesman_product_month['itemdesc'] = df_salesman_product_month['xitem'] + ':-' + df_salesman_product_month['xdesc']
df_salesman_product_month = df_salesman_product_month.pivot(index='spname',columns='itemdesc',values='xfinalqtydel')
df_salesman_product_month = df_salesman_product_month.reset_index()
df_salesman_product_month = df_salesman_product_month.rename(columns={'spname':'Salesman Name'})
df_salesman_product_month.loc['sum'] = df_salesman_product_month.sum(axis=0)
df_salesman_product_month = df_salesman_product_month.fillna(value=0,axis=1)
df_salesman_product_month.loc['sum','Salesman Name'] = 0

#datewise product sales
df_datewise_product = df_main.groupby(['xdate','xitem','xdesc']).sum()[['xfinalqtydel']]
df_datewise_product = df_datewise_product.reset_index()
df_datewise_product['itemdesc'] = df_datewise_product['xitem'] + df_datewise_product['xdesc']
df_datewise_product = df_datewise_product.pivot(index='xdate',columns='itemdesc',values='xfinalqtydel')
df_datewise_product = df_datewise_product.reset_index()
df_datewise_product['Month'] = pd.DatetimeIndex(df_datewise_product['xdate']).month
monthList = list(set(df_datewise_product['Month'].tolist()))
monthDict = {1:'January',2:'February',3:'March',4:'April',5:'May',6:'June',7:'July',8:'August',9:'September',10:'October',11:'November',12:'December'}

# for m in monthList:
#     df_datewise_product = df_datewise_product.append(df_datewise_product[df_datewise_product['Month']==m].sum(numeric_only=True),ignore_index=True)
#     df_datewise_product.at[df_datewise_product.index[-1],'xdate'] = monthDict[m]

df_datewise_product = df_datewise_product.fillna(value=0,axis=1)

#create area wise product sales
df_areawise_product = df_main.groupby(['xdiv','xitem','xdesc']).sum()[['xfinalqtydel']]
df_areawise_product = df_areawise_product.reset_index()
df_areawise_product['itemdesc'] = df_areawise_product['xitem'] + ':-' + df_areawise_product['xdesc']
df_areawise_product = df_areawise_product.pivot(index='xdiv',columns='itemdesc',values='xfinalqtydel')
df_areawise_product = df_areawise_product.reset_index()
df_areawise_product = df_areawise_product.rename(columns={'xdiv':'Area'})
df_areawise_product.loc['sum'] = df_areawise_product.sum(axis=0)
df_areawise_product = df_areawise_product.fillna(value=0,axis=1)
df_areawise_product.loc['sum','Area'] = 0

#Create Area-Customer wise product sales
df_customer_area_product = df_main.groupby(['xdiv','xcus','xshort','xitem','xdesc']).sum()[['xfinalqtydel']]
df_customer_area_product = df_customer_area_product.reset_index()
df_customer_area_product = pd.pivot_table(df_customer_area_product,index=['xdiv','xcus','xshort'],columns=['xitem','xdesc'],aggfunc = np.sum)
df_customer_area_product = df_customer_area_product.fillna(value=0,axis=1)

infoDictMonthly = {}

thisDay = datetime.now().day
thisDayName = datetime.now().strftime("%A")
if thisDay == 2 and thisDayName == 'Saturday' :
    thisMonth = datetime.now().month - 1
elif thisDay == 1:
    thisMonth = datetime.now().month - 1
else:
    thisMonth = datetime.now().month

df_month = df_main[df_main['Month']==thisMonth]

infoDictMonthly['Month'] = thisMonth

#salesman current month gross
gs = df_month.groupby(['xsp','xname']).sum()[['xfinallineamt']]
#highest grossing salesman
hgsList = list(gs['xfinallineamt'].idxmax())
hgsList.append(gs['xfinallineamt'].max())
infoDictMonthly['Salesman with Highest Gross Sales'] = hgsList

#Lowest grossing salesman
lgsList = list(gs['xfinallineamt'].idxmin())
lgsList.append(gs['xfinallineamt'].min())
infoDictMonthly['Salesman with Lowest Gross Sales'] = lgsList

#Area current Month Gross
ga = df_month.groupby(['xdiv']).sum()[['xfinallineamt']]

#Highest grossing Area
hgaList = [ga['xfinallineamt'].idxmax()]
hgaList.append(ga['xfinallineamt'].max())
infoDictMonthly['Area with Highest Gross Sales'] = hgaList

#Lowest Grossing Area
lgaList = [ga['xfinallineamt'].idxmin()]
lgaList.append(ga['xfinallineamt'].min())
infoDictMonthly['Area with Lowest Gross Sales'] = lgaList

#customer current month gross
gc = df_month.groupby(['xcus','xshort','xdiv']).sum()[['xfinallineamt']]

#highest grossing customer
hgcList = list(gc['xfinallineamt'].idxmax())
hgcList.append(gc['xfinallineamt'].max())
infoDictMonthly['Customer with Highest Gross Sales'] = hgcList

#lowest grossing customer
lgcList = list(gc['xfinallineamt'].idxmin())
lgcList.append(gc['xfinallineamt'].min())
infoDictMonthly['Customer with Lowest Gross Sales'] = lgcList

#item current month gross
gi = df_month.groupby(['xitem','xdesc'])[['xfinallineamt']].sum()

#highest grossing item
hgiList = list(gi['xfinallineamt'].idxmax())
hgiList.append(gi['xfinallineamt'].max())
infoDictMonthly['Item with Highest Gross Sales'] = hgiList

#Lowest Grossing Item
lgiList = list(gi['xfinallineamt'].idxmin())
lgiList.append(gi['xfinallineamt'].min())
infoDictMonthly['Item with Lowest Gross Sales'] = lgiList

#salesman current month unit sold
uss = df_month.groupby(['xsp','xname']).sum()[['xfinalqtydel']]

#highest unit selling salesman
hussList = list(uss['xfinalqtydel'].idxmax())
hussList.append(uss['xfinalqtydel'].max())
infoDictMonthly['Salesman with Highest Unit Sold'] = hussList

#Lowest unit selling salesman
lussList = list(uss['xfinalqtydel'].idxmin())
lussList.append(uss['xfinalqtydel'].min())
infoDictMonthly['Salesman with Lowest Unit Sold'] = lussList

#Area unit sold
usa = df_month.groupby(['xdiv']).sum()[['xfinalqtydel']]

#Highest unit sold Area
husaList = [usa['xfinalqtydel'].idxmax()]
husaList.append(usa['xfinalqtydel'].max())
infoDictMonthly['Area with Highest Unit Sold'] = husaList

#Lowest Unit Selling Area
lusaList = [usa['xfinalqtydel'].idxmin()]
lusaList.append(usa['xfinalqtydel'].min())
infoDictMonthly['Area with Lowest Unit Sold'] = lusaList

#customer unit sold this month
usc = df_month.groupby(['xcus','xshort','xdiv']).sum()[['xfinalqtydel']]

#highest unit sold to customer
huscList = list(usc['xfinalqtydel'].idxmax())
huscList.append(usc['xfinalqtydel'].max())
infoDictMonthly['Customer who bought Highest Units'] = huscList

#lowest unit sold to customer
luscList = list(usc['xfinalqtydel'].idxmin())
luscList.append(usc['xfinalqtydel'].min())
infoDictMonthly['Customer who bought Lowest Units'] = luscList

#item current month unit sold
usi = df_month.groupby(['xitem','xdesc'])[['xfinalqtydel']].sum()

#highest unit sold item
husiList = list(usi['xfinalqtydel'].idxmax())
husiList.append(usi['xfinalqtydel'].max())
infoDictMonthly['Item which had the Highest Units Sold'] = husiList

#Lowest unit sold Item
lusiList = list(usi['xfinalqtydel'].idxmin())
lusiList.append(usi['xfinalqtydel'].min())
infoDictMonthly['Item which had the Lowest Units Sold'] = lusiList

#Order number per salesman
ocs = df_month.groupby(['xsp','xname'])['xordernum'].nunique().to_frame()

#Highest Order Number of Salesman
hoscList = list(ocs['xordernum'].idxmax())
hoscList.append(ocs['xordernum'].max())
infoDictMonthly['Salesman with the Highest Number of Orders'] = hoscList

#Lowest Order Number of salesman
loscList = list(ocs['xordernum'].idxmin())
loscList.append(ocs['xordernum'].min())
infoDictMonthly['Salesman with the Lowest Number of Orders'] = loscList

#Average order number of Salesman
infoDictMonthly['Average Order Per Salesman'] = np.around(ocs['xordernum'].mean(),decimals=2)

#Order number per customer
occ = df_month.groupby(['xcus','xshort'])['xordernum'].nunique().to_frame()

#Highest Order Number of Customer
hoccList = list(occ['xordernum'].idxmax())
hoccList.append(occ['xordernum'].max())
infoDictMonthly['Customer who gave the Highest number of Orders'] = hoccList

#Lowest Order Number of Customer
loccList = list(occ['xordernum'].idxmin())
loccList.append(occ['xordernum'].min())
infoDictMonthly['Customer who gave the Lowest number of Orders'] = loccList

#Average order number of Customer
infoDictMonthly['Average Order Per Customer'] = np.around(occ['xordernum'].mean(),decimals=2)

#Order number per Area
oca = df_month.groupby(['xdiv'])['xordernum'].nunique().to_frame()

#Highest Order Number of Area
hocaList = [oca['xordernum'].idxmax()]
hocaList.append(oca['xordernum'].max())
infoDictMonthly['Area with the Highest Number of Orders'] = hocaList

#Lowest Order Number of Area
locaList = [oca['xordernum'].idxmin()]
locaList.append(oca['xordernum'].min())
infoDictMonthly['Area with the Lowest Number of Orders'] = locaList

#Average order number of Area
infoDictMonthly['Average Order Per Area'] = np.around(oca['xordernum'].mean(), decimals=2)

#Order number per Item
oci = df_month.groupby(['xitem','xdesc'])['xordernum'].nunique().to_frame()

#Highest Order Number of Item
hociList = list(oci['xordernum'].idxmax())
hociList.append(oci['xordernum'].max())
infoDictMonthly['Items with the Highest Number of Orders'] = hociList

#Lowest Order Number of Item
lociList = list(oci['xordernum'].idxmin())
lociList.append(oci['xordernum'].min())
infoDictMonthly['Item with the Lowest Number of Orders'] = lociList

#Average order number of Item
infoDictMonthly['Average Order Per Item'] = np.around(oci['xordernum'].mean(), decimals=2)

#Customer count in current month per area
cca = df_month.groupby(['xdiv'])['xcus'].nunique().to_frame()

#highest customer count Area
hccaList = [cca['xcus'].idxmax()]
hccaList.append(cca['xcus'].max())
infoDictMonthly['Area with the Highest Number of Customers'] = hccaList

#Lowest Customer Count Area
lccaList = [cca['xcus'].idxmin()]
lccaList.append(cca['xcus'].min())
infoDictMonthly['Area with the Lowest Number of Customers'] = lccaList

#Customer count in current month per Salesman
ccs = df_month.groupby(['xsp','xname'])['xcus'].nunique().to_frame()

#highest customer count per Salesman
hccsList = [ccs['xcus'].idxmax()]
hccsList.append(ccs['xcus'].max())
infoDictMonthly['Salesman with the Highest Number of Customers'] = hccsList

#Lowest Customer Count Salesman
lccsList = [ccs['xcus'].idxmin()]
lccsList.append(ccs['xcus'].min())
infoDictMonthly['Salesman with the Lowest Number of Customers'] = lccsList

#Customer count in current month per item
cci = df_month.groupby(['xitem','xdesc'])['xcus'].nunique().to_frame()

#highest customer count per Item
hcciList = [cci['xcus'].idxmax()]
hcciList.append(cci['xcus'].max())
infoDictMonthly['Items that were Distributed the Most'] = hcciList

#Lowest Customer Count Item
lcciList = [cci['xcus'].idxmin()]
lcciList.append(cci['xcus'].min())
infoDictMonthly['Items that were Distributed the Least'] = lcciList

#Total Number of Orders within the Month
infoDictMonthly['Total Number of Orders'] = len(df_month['xordernum'].unique())

#Total Number of Customers Sold to within the month
infoDictMonthly['Total Number of Customers'] = len(df_month['xcus'].unique())

#Total Number of Unit Sold of All Items
infoDictMonthly['Total Units Sold this Month'] = df_month['xfinalqtydel'].sum()

#Total Amount sold this month
infoDictMonthly['Total Amount Earned this Month'] = df_month['xfinallineamt'].sum()

#Item sold per area
ipa = df_month.groupby(['xdiv','xitem','xdesc'])[['xfinalqtydel']].sum()

#make the dictionary into a dataframe for email
dict_df = pd.DataFrame({key:pd.Series(value) for key, value in infoDictMonthly.items()})
dict_df = pd.melt(dict_df,var_name='All_Info')
dict_df = dict_df.groupby('All_Info')['value'].apply(list).to_frame().reset_index()
dict_df[[0,1,2,3]] = pd.DataFrame(dict_df.value.values.tolist(), index= dict_df.index)
dict_df = dict_df.drop('value',axis=1)
dict_df = dict_df.fillna(value=0,axis=1)

df_rectreturn = create_rectreturn(zid)
df_prmst = create_prmst(zid)
df_caitem = create_caitem(zid)
df_rectreturn = df_rectreturn.merge(df_prmst,on='xemp',how='left')
df_rectreturn = df_rectreturn.merge(df_caitem,on='xitem',how='left')
df_rectreturn = df_rectreturn[df_rectreturn['xper']==thisMonth]

rectsalesman = df_rectreturn.groupby(['xemp','xname'])[['xlineamt']].sum()
rectsalesmanarea = df_rectreturn.groupby(['xemp','xname','xarea'])[['xlineamt']].sum()
rectarea = df_rectreturn.groupby(['xarea'])[['xlineamt']].sum()
rectproductamt = df_rectreturn.groupby(['xitem','xdesc'])[['xlineamt']].sum()
rectproductqty = df_rectreturn.groupby(['xitem','xdesc'])[['xqtyord']].sum()

writer = pd.ExcelWriter('ZeptoSalesInformation.xlsx')
df_salesman_product.to_excel(writer,'Salesman_Product_Sales')
df_salesman_product_month.to_excel(writer,'Salesman_Product_Sales_Month')
df_datewise_product.to_excel(writer,'Datewise_Product_Sales')
df_areawise_product.to_excel(writer,'Areawise_Product_Sales')
df_customer_area_product.to_excel(writer,'Customer_perArea_Product_Sales')
writer.save()
writer.close()

writer2 = pd.ExcelWriter('ZeptoSalesRatios.xlsx')
gs.to_excel(writer2,'gs')
ga.to_excel(writer2,'ga')
gc.to_excel(writer2,'gc')
gi.to_excel(writer2,'gi')
uss.to_excel(writer2,'uss')
usa.to_excel(writer2,'usa')
usc.to_excel(writer2,'usc')
usi.to_excel(writer2,'usi')
ocs.to_excel(writer2,'ocs')
occ.to_excel(writer2,'occ')
oca.to_excel(writer2,'oca')
oci.to_excel(writer2,'oci')
cca.to_excel(writer2,'cca')
ccs.to_excel(writer2,'ccs')
cci.to_excel(writer2,'cci')
ipa.to_excel(writer2,'ipa')
dict_df.to_excel(writer2,'OverallSummary')
try:
    rectsalesman.to_excel(writer2,'rectsalesman')
    rectsalesmanarea.to_excel(writer2,'rectsalesmanarea')
    rectarea.to_excel(writer2,'rectarea')
    rectproductamt.to_excel(writer2,'rectproductamt')
    rectproductqty.to_excel(writer2,'rectproductqty')
except IndexError as error:
    print(error)
writer2.save()
writer2.close()

me = "pythonhmbr12@gmail.com"
#you = ["asaddat87@gmail.com","mo.hmbrbd@gmail.com","saleshmbrbd@gmail.com","zepto.sales1@gmail.com","ithmbrbd@gmail.com"]
you = ["ithmbrbd@gmail.com"]

msg = MIMEMultipart('alternative')
msg['Subject'] = "Zepto Sales Information"
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
with open('test.html','w') as f:
    f.write(HEADER)
    f.write(dict_df.to_html(classes='dict_df'))
    f.write(FOOTER)

filename = "test.html"
f = open(filename)
attachment = MIMEText(f.read(),'html')
msg.attach(attachment)

part = MIMEBase('application', "octet-stream")
part.set_payload(open("ZeptoSalesInformation.xlsx", "rb").read())
encoders.encode_base64(part)
part.add_header('Content-Disposition', 'attachment; filename="ZeptoSalesInformation.xlsx"')
msg.attach(part)

part = MIMEBase('application', "octet-stream")
part.set_payload(open("ZeptoSalesRatios.xlsx", "rb").read())
encoders.encode_base64(part)
part.add_header('Content-Disposition', 'attachment; filename="ZeptoSalesRatios.xlsx"')
msg.attach(part)

username = 'pythonhmbr12@gmail.com'
password = 'vksikttussvnbqef'

s = smtplib.SMTP('smtp.gmail.com:587')
s.starttls()
s.login(username, password)
s.sendmail(me,you,msg.as_string())
s.quit()


print("--- %s seconds ---" % (time.time() - start_time))
