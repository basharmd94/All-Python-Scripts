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

from dateutil.relativedelta import relativedelta
import random

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

#at the beginning estimate 27 days to find the number of challans for the month
approximate_days = 27

#read VAT analysis sheet make sure the format is maintained
month_df = pd.read_excel('vat_analysis_1.xlsx',engine='openpyxl').fillna(0)
month_df = month_df[(month_df.T != 0).any()]

#find the total amount in BDT sold of each product
month_df['s_amount'] = (month_df['l1_sale']*month_df['l1_price']) + (month_df['l2_sale']*month_df['l2_price']) + (month_df['l3_sale']*month_df['l3_price'])

#conditions for final price and lot 
conditions = [(month_df['l1_sale']!=0) & (month_df['l2_sale']==0) & (month_df['l3_sale']==0),
             (month_df['l1_sale']==0) & (month_df['l2_sale']!=0) & (month_df['l3_sale']==0),
             (month_df['l1_sale']==0) & (month_df['l2_sale']==0) & (month_df['l3_sale']!=0),
             (month_df['l1_sale']!=0) & (month_df['l2_sale']!=0) & (month_df['l3_sale']==0),
             (month_df['l1_sale']!=0) & (month_df['l2_sale']==0) & (month_df['l3_sale']!=0),
             (month_df['l1_sale']==0) & (month_df['l2_sale']!=0) & (month_df['l3_sale']!=0),
             (month_df['l1_sale']!=0) & (month_df['l2_sale']!=0) & (month_df['l3_sale']!=0)]
choices = [
           month_df['l1_price'],
           month_df['l2_price'],
           month_df['l3_price'],
           month_df['l1_price'],
           month_df['l1_price'],
           month_df['l2_price'],
           month_df['l1_price']
          ]
choices_lot = [
           'l1_sale',
           'l2_sale',
           'l3_sale',
           'l1_sale',
           'l1_sale',
           'l2_sale',
           'l1_sale'
           ]

month_df['final_price'] = np.select(conditions,choices,default=0)
month_df['lot'] = np.select(conditions,choices_lot,default=0)
month_df = month_df[month_df['lot']!='0']

# finding the total sales of all the products summed together for the month to find ratios
total_sales = (month_df['l1_sale']*month_df['l1_price']).sum() + (month_df['l2_sale']*month_df['l2_price']).sum() + (month_df['l3_sale']*month_df['l3_price']).sum()

item_df = pd.DataFrame(columns = ['Product Name',
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

#create new challan values ranging from 5000BDT to 70000 BDT
chl_number = 1500
start = 5000
end = 70000

#create the initial chl_value_list which includes all the chllan values to be used within the month
chl_value_list = rand(start,end,chl_number,total_sales)
#we faced a problem where the random generation took the value higher than the intended sales value this list correction takes the 
#max and subtracts the difference to make the sum of the list eqaul to that given in the VAT analysis sheet
chl_value_list = list_correction(total_sales,chl_value_list)
value_list_len = len(chl_value_list)
d = pd.Series(chl_value_list)
chl_value_df = pd.DataFrame(d).rename(columns={0:'chl_value'})

#create random number of entries by taking the average of a day by dividing length of the challan value list by 27
#then find random number which are between 70 to 20 percent of the average value
average_per_day = value_list_len/approximate_days
high = round((average_per_day + (average_per_day*0.7)),0)
low = round((average_per_day - (average_per_day*0.2)),0)
chl_count_list = rand_cust(low,high,approximate_days,value_list_len)
#list correct the same way if the total is not equal to the intended value the max is subtracted from or the min is added to
chl_count_list = list_correction(value_list_len,chl_count_list)
#also if the length of 27 is not satisfied any count within this list which is divisible by 2 is divided into 2 parts until the list count of 27 is satisfied
chl_count_list = fix_list(chl_count_list,approximate_days)
random.shuffle(chl_count_list)

e = pd.Series(chl_count_list)
chl_count_df = pd.DataFrame(e).rename(columns={0:'chl_count'})

count_list_sum = sum(chl_count_list)
f = pd.Series(count_list_sum)
chl_sum_df = pd.DataFrame(f).rename(columns={0:'chl_sum'})

with pd.ExcelWriter('chl_value_list.xlsx') as writer:  
    chl_value_df.to_excel(writer, sheet_name='chl_value_list')
    chl_count_df.to_excel(writer, sheet_name='chl_count_list')
    chl_sum_df.to_excel(writer,sheet_name='chl_count_sum')
    month_df.to_excel(writer,sheet_name='vat_analysis')
    item_df.to_excel(writer,sheet_name='item_ledger')
