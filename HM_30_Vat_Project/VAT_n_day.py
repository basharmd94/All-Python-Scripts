from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from datetime import date,datetime,timedelta
import openpyxl
import random
import os
import datetime
import shutil
import re
# use Project config
from dotenv import load_dotenv
from sqlalchemy import text  # ← Required for parameterized queries with SQLAlchemy
import sys

# ─────────────────────────────────────────────────────────────────────
# 🌍 1. Load Environment & Setup
# ─────────────────────────────────────────────────────────────────────
load_dotenv()

# ─────────────────────────────────────────────────────────────────────
# 🧩 2. Add Root & Import Shared Modules
# ─────────────────────────────────────────────────────────────────────
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from project_config import engine
from mail import send_mail, get_email_recipients




logs = {}

def rand_simple(start,end):
    num = np.random.randint(start, end)
    return num

def rand(start, end, num, total_sales):
    res = []
    for j in range(num):
        if sum(res)<total_sales:
            res.append(np.random.randint(start, end))
    return res

def rand_cust(start,end,num, total_chl):
    res = []
    for j in range(num):
        if (sum(res)<total_chl) & (len(res)<num):
            res.append(np.random.randint(start, end))
    return res

def get_sales(zid,date):
    df = pd.read_sql("""SELECT imtrn.xcus,cacus.xshort,cacus.xadd1,cacus.xadd2, imtrn.xyear, imtrn.xper, imtrn.xdate
                        FROM imtrn
                        JOIN cacus
                        ON imtrn.xcus = cacus.xcus
                        WHERE imtrn.zid = '%s'
                        AND cacus.zid = '%s'
                        AND imtrn.xdate = '%s'
                        AND imtrn.xdoctype = '%s'
                        GROUP BY imtrn.xcus,imtrn.xdate,imtrn.xyear,imtrn.xper,cacus.xshort,cacus.xadd1,cacus.xadd2
                        ORDER BY imtrn.xdate"""%(zid,zid,date,'DO--'),con=engine)
    return df

def list_correction(l1,l2):
    if l1 != sum(l2):
        if l1<sum(l2): 
            l2[l2.index(max(l2))] = max(l2)+(l1-sum(l2))
        elif l1>sum(chl_count_list):
            l2[l2.index(min(l2))] = min(l2)+(l1-sum(l2))
    return l2

def fix_list(oi_list,num):
    for x in oi_list:
        if len(oi_list) == num:
            break
        elif x % 2 == 0:
            del oi_list[oi_list.index(x)]
            for i in range(2):
                oi_list.append(x/2)
    oi_list = [int(x) for x in oi_list]
    random.shuffle(oi_list)
    return oi_list

month_change = input(str('y or n: ')) #or n manual entry (check if not n or y )
manual_date = ""
input_what = True

while input_what:
    input_date = input("please input date---  ")
    acknowledge = input(str(f"your input is [ {input_date}] . if Alright press [y] else press [n]---  "))
    if acknowledge == "y":
        manual_date = input_date
        input_what = False
    else:
        print("you selected no. Lets try again with input date ")
        input_date
        
        
manual_date_year = int(manual_date.split('-')[0])
zid = 100001

print(manual_date)
logs.update({'date' : manual_date})

#read VAT analysis sheet
month_df = pd.read_excel('chl_value_list.xlsx',sheet_name='vat_analysis',engine='openpyxl').fillna(0).rename(columns={'Unnamed: 0':'sl_no'})
# drop repetetive sl_no columns
try:
    month_df = month_df[month_df.columns.drop(list(month_df.filter(regex='sl_no')))]
except:
    pass
month_df = month_df[(month_df.T != 0).any()]
logs.update({'First Read month_df sum' : month_df['s_amount'].sum() })
#     print(month_df['s_amount'].sum(),'First Read month_df sum')
month_df_t = month_df
#     print(month_df_t['s_amount'].sum(),'First Read month_df_t sum')
logs.update({'First Read month_df_t sum' : month_df_t['s_amount'].sum() })

#read the item_df format for new month should be empty for running month should be running
item_df =  pd.read_excel('chl_value_list.xlsx',sheet_name='item_ledger',engine='openpyxl')
# drop repetetive unnamed 0 columns
try:
    item_df = item_df[item_df.columns.drop(list(item_df.filter(regex='Unnamed')))]
#         print(item_df['(16) Taxable Value'].sum(),'First Read item_df sum')
    logs.update({'First Read item_df sum' : item_df['(16) Taxable Value'].sum() })
except:
    pass


# finding the total sales of all the products summed together for the month to find ratios this will change everytime a new day is processed
total_sales = (month_df_t['l1_sale']*month_df_t['l1_price']).sum() + (month_df_t['l2_sale']*month_df_t['l2_price']).sum() + (month_df_t['l3_sale']*month_df_t['l3_price']).sum()
#     print(total_sales,'s_amount total calculation')
logs.update({'s_amount total calculation' :total_sales })
# this is the individual challan totals used in conjunction with challan count to find the number of challans to process per day
chl_value_df = pd.read_excel('chl_value_list.xlsx',sheet_name='chl_value_list',engine='openpyxl')
chl_value_list = chl_value_df['chl_value'].to_list()
#     print(chl_value_list,'chl_value_list read')
#     print(len(chl_value_list),'chl_value_list length')
logs.update({'chl_value_list read' :chl_value_list , 'chl_value_list length' : len(chl_value_list)})
#this is the count of number of challans per day updated each time its used
chl_count_df = pd.read_excel('chl_value_list.xlsx',sheet_name='chl_count_list',engine='openpyxl')
chl_count_list = chl_count_df['chl_count'].to_list()
#     print(chl_count_list,'chl_count_list read')
#     print(len(chl_count_list),'chl_count_list length')
logs.update({'chl_count_list read' :chl_count_list , 'chl_count_list length' : len(chl_count_list)})
#this is an total of all the challans that are supposed to be created in the entire month (its an estimated value)
chl_sum_df = pd.read_excel('chl_value_list.xlsx',sheet_name='chl_count_sum',engine='openpyxl')
try:
    chl_sum_df = chl_sum_df[chl_sum_df.columns.drop(list(chl_sum_df.filter(regex='Unnamed')))]
except:
    pass


chl_sum = chl_sum_df['chl_sum'].to_list()[0]
#     print(chl_sum,'chl_sum read')
logs.update({'chl_sum read' :chl_sum })

#no_challan signifies the number of challans for that day take from the challan count list which is updated (meaning the element that is used from this list is removed) and saved for the next day
if month_change == 'n':
    no_challan = chl_count_list[0]
#         print(no_challan,'no_challan used on this day')
    logs.update({'no_challan used on this day' :no_challan })
    chl_count_list = chl_count_list[1:]
#         print(chl_count_list,'chl_count_list after use')
#         print(len(chl_count_list),'chl_count_list length after use')
    logs.update({'chl_count_list after use' :chl_count_list , 'chl_count_list length after use' :len(chl_count_list)  })
    e = pd.Series(chl_count_list)
    chl_count_df = pd.DataFrame(e).rename(columns={0:'chl_count'})
elif month_change == 'y':
    #if this is the end of the month how many ever challan values we have left needs to be used up, we approximated the number of challan using 27 but we might have holidays and what not so....
    no_challan = len(chl_value_list)
#         print(no_challan,'no_challan used on last day')
    logs.update({'no_challan used on last day' :no_challan   })

#get the sales information from the database for that day, particularly customer information
sales_df = get_sales(zid,manual_date)
#     print(len(sales_df),'length of sales df')
#create a temp challan value list to get the exact number of customer names as challan values we need for the day
chl_value_list_n = chl_value_list[:no_challan]
#     print(chl_value_list_n,'chl_value_list_n creation with no_challan')
#     print(len(chl_value_list_n),'chl_value_list_n length')

logs.update({'chl_value_list_n creation with no_challan' :chl_value_list_n, 'chl_value_list_n length' : chl_value_list_n})

sales_df = sales_df.sample(n=len(chl_value_list_n))
sales_df['sale'] = chl_value_list_n
#     print(f"{len(sales_df)},'length of sales df of samples'\n")
logs.update({'length of sales df of samples':len(sales_df)})

#create chllan dict to loop thourgh late it should just include the amount needed to be sold to the customer and their address and location etc.
chl_dict = sales_df.groupby(['xshort']).apply(lambda x: x[['sale','xadd1','xadd2']].values.tolist()[0]).to_dict()
#     print(f"{len(chl_dict)},'length of chl_dict of sales people samples'\n")
logs.update({'length of chl_dict of sales people samples':len(chl_dict)})

#finalize the challan value list by eliminating all the values in the list that you used for the day
chl_value_list = chl_value_list[no_challan:] ### will be update below with qty
#     print(chl_value_list,'chl_value_list creation with no_challan after use')
#     print(len(chl_value_list),'chl_value_list length after use')
logs.update({'chl_value_list length after use': len(chl_value_list), 'chl_value_list creation with no_challan after use' : chl_value_list  })
f = pd.Series(chl_value_list)
chl_value_df = pd.DataFrame(f).rename(columns={0:'chl_value'})


#we need empty dicts and list for the challans & itemledgers to save
chl_dict_final = {}
item_df_list = []
residual_chl = [] #### new update


#start the loop that goes through chl dict and assigns random products from VAT analysis sheet to meet value(s) from challan value list
for key,value in chl_dict.items():
    chl_value = value[0]
    # f.write(f"{chl_value},'chl_value for every iter' ")
    logs.update({'chl_value for every iter_'+key:chl_value}) # log dict
#         print(chl_value,'chl_value for every iter')
    chl_add1 = value[1]
    chl_add2 = value[2]
    random_sample = rand_simple(3,12)
    print(random_sample,'random_sample')
    #sample random products from the main month_df that had been updated in the last run(yesterday or something) or new if its that start of the month
    sample_df = month_df_t.sample(n=random_sample,weights = 't_sale')
    #find the % each product holds in this sample list by dividing value of each product by total sum of sample value amount(s) basically putting weights on the samples chosen
    sample_df['value_ratio'] = sample_df['s_amount']/sample_df['s_amount'].sum()
    #multiply the value ratio by one of the values in the challan value list 
    sample_df['chl_value'] = (sample_df['value_ratio'] * chl_value).round(0)
    #finding the qty of the product by dividing the amount by the price
    sample_df['chl_sale_qty'] = (sample_df['chl_value']/sample_df['final_price']).round(0)

    #these two rounding the 2 former lines could be a major problem at the end of the month ####
    #chl_sale_qty needs to be adjusted where the logic is as follows if chl_sale_qty is greater than t_sale and greater than l1_sale or greater than l2 sale chl_sale_qty = l1_sale or l2 sale 
    condition_sale_qty = [(sample_df['chl_sale_qty']>sample_df['l1_sale']) & (sample_df['l1_sale']>0) & (sample_df['l2_sale']==0) & (sample_df['l3_sale']==0),
                          (sample_df['chl_sale_qty']>sample_df['l2_sale']) & (sample_df['l2_sale']>0) & (sample_df['l1_sale']==0) & (sample_df['l3_sale']==0),
                          (sample_df['chl_sale_qty']>sample_df['l3_sale']) & (sample_df['l3_sale']>0) & (sample_df['l2_sale']==0) & (sample_df['l1_sale']==0)]
    choices_sale_qty = [sample_df['l1_sale'],
                       sample_df['l2_sale'],
                       sample_df['l3_sale']]

    sample_df['chl_sale_qty'] = np.select(condition_sale_qty,choices_sale_qty,default=sample_df['chl_sale_qty'])

    # print(sample_df[(sample_df['chl_sale_qty']>sample_df['l1_sale']) & (sample_df['chl_sale_qty']>sample_df['l2_sale'])])

    residual_chl_value = chl_value - (sample_df['chl_sale_qty']*sample_df['final_price']).sum()
    if residual_chl_value != 0:
        residual_chl.append(residual_chl_value)

    # make a temporary month_df and equate later
    month_df_t = month_df_t.merge(sample_df[['Product Name','chl_sale_qty']],on='Product Name',how='left').fillna(0)

    #conditions for l sales and stock, this was extremely confusing but we need to track and improve over time
    conditions_l1 = [(month_df_t['l1_sale']!=0) & (month_df_t['l2_sale']==0) & (month_df_t['l3_sale']==0) & (month_df_t['l1_sale']>=month_df_t['chl_sale_qty']),
                 (month_df_t['l1_sale']!=0) & (month_df_t['l2_sale']!=0) & (month_df_t['l3_sale']==0) & (month_df_t['l1_sale']>=month_df_t['chl_sale_qty']),
                 (month_df_t['l1_sale']!=0) & (month_df_t['l2_sale']==0) & (month_df_t['l3_sale']!=0) & (month_df_t['l1_sale']>=month_df_t['chl_sale_qty']),
                 (month_df_t['l1_sale']!=0) & (month_df_t['l2_sale']!=0) & (month_df_t['l3_sale']!=0)& (month_df_t['l1_sale']>=month_df_t['chl_sale_qty'])]

    conditions_l2 = [(month_df_t['l1_sale']==0) & (month_df_t['l3_sale']==0) & (month_df_t['l2_sale']!=0) & (month_df_t['l2_sale']>=month_df_t['chl_sale_qty']),
                    (month_df_t['l1_sale']>=0) &(month_df_t['l1_sale']<month_df_t['chl_sale_qty']) & (month_df_t['l3_sale']>=0) & (month_df_t['l2_sale']!=0) & (month_df_t['l2_sale']>=month_df_t['chl_sale_qty'])]

    conditions_l3 = [(month_df_t['l1_sale']==0) & (month_df_t['l2_sale']==0) & (month_df_t['l3_sale']!=0) & (month_df_t['l3_sale']>=month_df_t['chl_sale_qty']),
                     (month_df_t['l1_sale']>=0) & (month_df_t['l1_sale']<month_df_t['chl_sale_qty']) & (month_df_t['l2_sale']>=0) & (month_df_t['l3_sale']!=0) & (month_df_t['l3_sale']>=month_df_t['chl_sale_qty'])]

    #conditions for final price essentially to delete product not required anymore from month_df
    conditions_price = [(month_df_t['l1_sale']!=0) & (month_df_t['l2_sale']==0) & (month_df_t['l3_sale']==0) & (month_df_t['l1_sale']>=month_df_t['chl_sale_qty']),
                        (month_df_t['l1_sale']!=0) & (month_df_t['l2_sale']!=0) & (month_df_t['l3_sale']==0) & (month_df_t['l1_sale']>=month_df_t['chl_sale_qty']),
                        (month_df_t['l1_sale']!=0) & (month_df_t['l2_sale']==0) & (month_df_t['l3_sale']!=0) & (month_df_t['l1_sale']>=month_df_t['chl_sale_qty']),
                        (month_df_t['l1_sale']!=0) & (month_df_t['l2_sale']!=0) & (month_df_t['l3_sale']!=0)& (month_df_t['l1_sale']>=month_df_t['chl_sale_qty']),
                        (month_df_t['l1_sale']==0) & (month_df_t['l3_sale']==0) & (month_df_t['l2_sale']!=0) & (month_df_t['l2_sale']>=month_df_t['chl_sale_qty']),
                        (month_df_t['l1_sale']>=0) &(month_df_t['l1_sale']<month_df_t['chl_sale_qty']) & (month_df_t['l3_sale']>=0) & (month_df_t['l2_sale']!=0) & (month_df_t['l2_sale']>=month_df_t['chl_sale_qty']),
                        (month_df_t['l1_sale']==0) & (month_df_t['l2_sale']==0) & (month_df_t['l3_sale']!=0) & (month_df_t['l3_sale']>=month_df_t['chl_sale_qty']),
                        (month_df_t['l1_sale']>=0) & (month_df_t['l1_sale']<month_df_t['chl_sale_qty']) & (month_df_t['l2_sale']>=0) & (month_df_t['l3_sale']!=0) & (month_df_t['l3_sale']>=month_df_t['chl_sale_qty'])
                       ]

    #choices for l sales and stock
    choices_sale_l1 = [
               month_df_t['l1_sale']-month_df_t['chl_sale_qty'],
               month_df_t['l1_sale']-month_df_t['chl_sale_qty'],
               month_df_t['l1_sale']-month_df_t['chl_sale_qty'],
               month_df_t['l1_sale']-month_df_t['chl_sale_qty']
            ]

    choices_stock_l1 = [
               month_df_t['l1_stock']-month_df_t['chl_sale_qty'],
               month_df_t['l1_stock']-month_df_t['chl_sale_qty'],
               month_df_t['l1_stock']-month_df_t['chl_sale_qty'],
               month_df_t['l1_stock']-month_df_t['chl_sale_qty']
            ]

    choices_sale_l2 = [
               month_df_t['l2_sale']-month_df_t['chl_sale_qty'],
               month_df_t['l2_sale']-month_df_t['chl_sale_qty']
            ]

    choices_stock_l2 = [
               month_df_t['l2_stock']-month_df_t['chl_sale_qty'],
               month_df_t['l2_stock']-month_df_t['chl_sale_qty']
            ]

    choices_sale_l3 = [
               month_df_t['l3_sale']-month_df_t['chl_sale_qty'],
               month_df_t['l3_sale']-month_df_t['chl_sale_qty']
            ]

    choices_stock_l3 = [
               month_df_t['l3_stock']-month_df_t['chl_sale_qty'],
               month_df_t['l3_sale']-month_df_t['chl_sale_qty'],
            ]

    #choices for final proce
    choices_price = [month_df_t['l1_price'],
                     month_df_t['l1_price'],
                     month_df_t['l1_price'],
                     month_df_t['l1_price'],
                     month_df_t['l2_price'],
                     month_df_t['l2_price'],
                     month_df_t['l3_price'],
                     month_df_t['l3_price']]

    #subtract qty from month df and update
    month_df_t['l1_sale'] = np.select(conditions_l1,choices_sale_l1,default=0)
    month_df_t['l1_stock'] = np.select(conditions_l1,choices_stock_l1,default=0)
    month_df_t['l2_sale'] = np.select(conditions_l2,choices_sale_l2,default=month_df_t['l2_sale'])
    month_df_t['l2_stock'] = np.select(conditions_l2,choices_stock_l2,default=month_df_t['l2_stock'])
    month_df_t['l3_sale'] = np.select(conditions_l3,choices_sale_l3,default=month_df_t['l3_sale'])
    month_df_t['l3_stock'] = np.select(conditions_l3,choices_stock_l3,default=month_df_t['l3_stock'])


    #change final price and calculate s_amount before closing the file & save to excel as vat_analysis sheet this will help keep the sample % accurate in hte next run
    month_df_t['final_price'] = np.select(conditions_price,choices_price,0)
    month_df_t = month_df_t[month_df_t['final_price']!=0]
    month_df_t['s_amount'] = (month_df_t['l1_sale']*month_df_t['l1_price']) + (month_df_t['l2_sale']*month_df_t['l2_price']) + (month_df_t['l3_sale']*month_df_t['l3_price'])

    #need to remove chl_sale qty for the next iteration
    month_df_t = month_df_t.drop('chl_sale_qty',axis=1)
#         print(month_df_t['s_amount'].sum(),'s_amount after sample use')
    logs.update({'s_amount after sample use_'+key:month_df_t['s_amount'].sum()}) # log dict

    #create the chl_df for every loop 
    chl_df = pd.DataFrame(columns = [
                                     'Description of Goods / Services (including Brand name if applicable)',
                                     'Unit of Supply',
                                     'Quantity',
                                     'Unit Price',
                                     'Total Value',
                                     'Rate of Supplementary Duty',
                                     'Supplementary Duty',
                                     'VAT Rate',
                                     'VAT',
                                     'VAT including SP and VAT'])

    #Assign value from sample_df to chl_df
    chl_df['Description of Goods / Services (including Brand name if applicable)'] = sample_df['Product Name']
    chl_df['Unit of Supply'] = sample_df['unit_supply']
    chl_df['Quantity'] = sample_df['chl_sale_qty']
    chl_df['Unit Price'] = sample_df['final_price']
    chl_df['Total Value'] = sample_df['final_price']*sample_df['chl_sale_qty']
    chl_df['Rate of Supplementary Duty'] = '-'
    chl_df['Supplementary Duty'] = '-'
    chl_df['VAT Rate'] = '15%'
    chl_df['VAT'] = (chl_df['Total Value']*0.15).round(1)
    chl_df['VAT including SP and VAT'] = chl_df['Total Value'] + chl_df['VAT']


    #create add_df for each iteration which holds customer information
    add_df = pd.DataFrame(columns = [
                                     'Customer Name',                       
                                    'Customer BIN',
                                    'Customer address',
                                    'Distribution Location',
                                    'Car no.'])
    add_df.loc[len(add_df.index)] = [key,'',chl_add1,chl_add2,'']


    #create misc_df for each iteration which holds challan information
    misc_df = pd.DataFrame(columns=['Challan no.',
                                   'Issue Date',
                                   'Issue Time'])
    misc_df.loc[len(misc_df.index)] = ['',manual_date,'']

    #create the item ledger for each iteration
    item_df_t = pd.DataFrame(columns = [  #'SL. No', 'Product Name'
                                 'Product Name',
                                 '(2)Date', #from challan
                                 '(3) Opening Stock of Goods/Services Quantity(Unit)', # Self calculate
                                 '(4) Value (Excluding all types of Taxes)',# Self Calculate
                                 '(5) Production Quantity(unit)', # No need
                                '(6) Value (Excluding all types of Taxes)',#no need
                                '(7) = (3)+(5) Total Produced Goods/Services Quantity (Unit)', #Self calculate
                                '(8) = (4)+(6) Value (Excluding all types of Taxes)',#self calculate
                                '(9) Buyer/Supplier Receipt Name',# Required bring chl_df name 
                                '(10) Address', # Chl_df xadd2
                                '(11) Registration/Enlisted/National ID No.', # No need
                                '(12) Challan Details Number', #manual 
                                '(13) Date', #misc_df date
                                '(14) Sold/Supplied Goods Description',#chl_df Product Qty
                                '(15) Quantity', #chl_df Quantity
                                '(16) Taxable Value', #chl_df Total Value
                                '(17) Supplementary Duty(If Have)', #No need
                                '(18) VAT', #chl_df VAT
                                '(19) = (7)-(15) Closing Balance of Material Quantity(Unit)', #Self Calculate
                                '(20) Value (Excluding all types of Taxes)', #Self Calculate
                                '(21) Comments' #No need
                                ]
                      )

    #populate the item ledger with the chl_df from this iteration and append to the item_df_list for appending later
    item_df_t['Product Name'] = chl_df['Description of Goods / Services (including Brand name if applicable)']
    item_df_t['(2)Date'] = misc_df['Issue Date'][0]#pd.Series([misc_df['Issue Date'][0] for x in range(len(item_df_t.index))]), #from challan
    item_df_t['(3) Opening Stock of Goods/Services Quantity(Unit)'] = 0 # Self calculate
    item_df_t['(4) Value (Excluding all types of Taxes)'] = 0 # Self Calculate
    item_df_t['(5) Production Quantity(unit)'] = '-' # No need
    item_df_t['(6) Value (Excluding all types of Taxes)'] = '-' #no need
    item_df_t['(7) = (3)+(5) Total Produced Goods/Services Quantity (Unit)'] = '0' #Self calculate
    item_df_t['(8) = (4)+(6) Value (Excluding all types of Taxes)'] = '0' #self calculate
    item_df_t['(9) Buyer/Supplier Receipt Name'] = add_df['Customer Name'][0] # Required bring chl_df name 
    item_df_t['(10) Address'] = add_df['Distribution Location'][0] # Chl_df xadd2
    item_df_t['(11) Registration/Enlisted/National ID No.'] = '-' # No need
    item_df_t['(12) Challan Details Number',] = '-' #manual 
    item_df_t['(13) Date'] = misc_df['Issue Date'][0] #misc_df date
    item_df_t['(14) Sold/Supplied Goods Description'] = chl_df['Description of Goods / Services (including Brand name if applicable)']#chl_df Product Qty
    item_df_t['(15) Quantity'] = chl_df['Quantity'] #chl_df Quantity
    item_df_t['(16) Taxable Value'] = chl_df['Total Value'] #chl_df Total Value
    item_df_t['(17) Supplementary Duty(If Have)'] = '-' #No need
    item_df_t['(18) VAT'] = chl_df['VAT'] #chl_df VAT
    item_df_t['(19) = (7)-(15) Closing Balance of Material Quantity(Unit)'] = 0 #Self Calculate
    item_df_t['(20) Value (Excluding all types of Taxes)'] = 0 #Self Calculate
    item_df_t['(21) Comments'] =  '-'#No need
#         print(item_df_t['(16) Taxable Value'].sum(),'item_df_t Taxable value sum each iter')
    logs.update({'item_df_t Taxable value sum each iter_'+key : item_df_t['(16) Taxable Value'].sum()})
    item_df_list.append(item_df_t)

    add_df = add_df.transpose()
    misc_df = misc_df.transpose()
#         print(chl_df['Total Value'].sum(),'each chl_df total value sum')
#         print(len(chl_df),'length of each chl_df')
    logs.update({'each chl_df total value sum_'+key:chl_df['Total Value'].sum() , 'gth of each chl_df' : len(chl_df) }) #log dict

    #find the total values for this iteration of chl_df
    chl_df.insert(loc=0, column='Sl No.', value=np.arange(len(chl_df))+1)
    # chl_df.loc[len(chl_df.index),:] = chl_df.sum(axis=0,numeric_only=True)
    # chl_df.at[len(chl_df.index)-1,'Unit Price'] = 0
    # chl_df.at[len(chl_df.index)-1,'Quantity'] = 0
    # chl_df.at[len(chl_df.index)-1,'Unit of Supply'] = 'Total'
    chl_df = chl_df.fillna(0)

    frames = [add_df,misc_df,chl_df]

    #save each chl_df from each iteration to a dict for use later
    chl_dict_final[key] = frames

residual_chl_ext = [sum(i) for i in zip(residual_chl,chl_value_list)]
residual_chl_ext.extend(chl_value_list[len(residual_chl_ext):])
chl_value_list = residual_chl_ext
#     print(chl_value_list,'after residual addition')
# print(len(item_df_list),'length of item df_list')
# f.write(f"{len(item_df_list)},'length of item df_list'\n")
logs.update({"length of item df_list":len(item_df_list)})       # log dict
#append all the item_df together
item_df_final = item_df_list[0].append([x for x in item_df_list[1:]])
item_df_final = item_df_final.sort_values(by = 'Product Name')
item_df = item_df.append(item_df_final)

logs.update({"Last Read item_df sum":item_df['(16) Taxable Value'].sum(), "Last Read item_df length" : len(item_df) }) 
# f.write(f"{item_df['(16) Taxable Value'].sum()},'Last Read item_df sum'\n")
# f.write(f"{len(item_df)},'Last Read item_df length'\n")
# print(item_df['(16) Taxable Value'].sum(),'Last Read item_df sum')
# print(len(item_df),'Last Read item_df length')
#append all month df together
month_df = month_df_t
logs.update({'Last Read month_df sum':month_df['s_amount'].sum(), 'Last Read month_df length' : len(month_df) })

#     print(f"{month_df['s_amount'].sum()},'Last Read month_df sum'\n")
#     print(f"{len(month_df)},'Last Read month_df length'\n")
# print(month_df['s_amount'].sum(),'Last Read month_df sum')
# print(len(month_df),'Last Read month_df length')
#take the final chllans created for the day and save them to a folder later email them to the vat officer

# create 
today = datetime.datetime.today()-timedelta(1)
# we need to create autofolder using the date
today_strf_date = today.strftime('%Y-%m-%d--%H-%M-%S')+ "-" + str(random.randint(20,255))
# checking if the directory email_folder 
# exist or not.
email_folder = f"email_folder/{today_strf_date}"
if not os.path.isdir(email_folder):
    # if the email_folder directory is 
    # not present then create it.
    os.makedirs(email_folder)

for key,value in chl_dict_final.items():
    try:
        key = re.sub('[^0-9a-zA-Z]+','',key)
        key = key[0:20]
        key = key.replace("/" , "")
    except:
        key = key.strip()
        key = key[0:20]
        key = key.replace("/" , "")

    with pd.ExcelWriter('all_challan/challan_' + "_" + key + '-' + manual_date + '.xlsx') as writer:
        value[0].to_excel(writer,sheet_name=key+manual_date,startrow=0,startcol=0)
        value[1].to_excel(writer,sheet_name=key+manual_date,startrow=0,startcol=10)
        value[2].to_excel(writer,sheet_name=key+manual_date,startrow=8,startcol=0)
################### this command is for save the excel files in email folder dirctory ###
    with pd.ExcelWriter(f'{email_folder}/challan_' + "_" + key + '-' + manual_date + '.xlsx') as writer:
        value[0].to_excel(writer,sheet_name=key+manual_date,startrow=0,startcol=0)
        value[1].to_excel(writer,sheet_name=key+manual_date,startrow=0,startcol=10)
        value[2].to_excel(writer,sheet_name=key+manual_date,startrow=8,startcol=0)


#save these files into email folder for sending specific days mail
with pd.ExcelWriter('chl_value_list.xlsx') as writer:  
    chl_value_df.to_excel(writer, sheet_name='chl_value_list')
    chl_count_df.to_excel(writer, sheet_name='chl_count_list')
    chl_sum_df.to_excel(writer,sheet_name='chl_count_sum')
    item_df.to_excel(writer,sheet_name='item_ledger')
    month_df.to_excel(writer,sheet_name='vat_analysis')

# copy challan value list to email folder
source = 'chl_value_list.xlsx'
destination = f'{email_folder}/chl_value_list.xlsx'
shutil.copy(source, destination)
# copy log.txt to email folder



# export all logs to txt file
file=  open ("log.txt" , 'a')
file.write(f"-------------------------------{manual_date}---------------------------------\n")
for k, v in logs.items():
    file.write (f"{k} : {v} \n\n")
file.write(f"-------------------------------{'END'}---------------------------------\n\n\n")
file.close()

#copy log.txt to email folder
source_log = 'log.txt'
destination_log = f'{email_folder}/log.txt'
shutil.copy(source_log, destination_log)


# take all file list in email folder
file_list = os.listdir(email_folder)
file_list = [email_folder + "/" + i for i in file_list]

# send all file attached with email
send_mail(
f"HM_30 vat analysis of {today_strf_date}",
 "please see the attachment",
attachment=file_list,
recipient = ['ithmbrbd@gmail.com','financecorp01@gmail.com'])
##process emails for the chl_df excel files & the item_df/itemledger sheet excel file to milon after processing
