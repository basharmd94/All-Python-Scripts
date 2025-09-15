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

def process_business_data(zid, business_name, start_date, end_date, use_xdtwotax=False, include_returnvalue=True):
    """
    Unified function to process business data for all companies
    
    Parameters:
    - zid: Business ID
    - business_name: Name for the business in output dict
    - start_date, end_date: Date range
    - use_xdtwotax: If True, uses xdtwotax instead of xlineamt (for Zepto)
    - include_returnvalue: If True, includes returnvalue in final_cost calculation
    
    Returns:
    - tuple: (summary_dict, df_final) - summary for main_data_dict and full DataFrame for Excel export
    """
    try:
        # Get sales data
        df_sales = get_sales_COGS(zid, start_date, end_date)
        sales_field = 'xdtwotax' if use_xdtwotax else 'xlineamt'
        df_sales = df_sales.groupby(['xitem','xdesc'])[['totalvalue', sales_field]].sum().reset_index().round(1)
        
        # Get return data
        df_return = get_return(zid, start_date, end_date)
        return_fields = ['returnvalue', 'totamt'] if include_returnvalue else ['totamt']
        
        if len(df_return) > 0:
            df_return = df_return.groupby(['xitem'])[return_fields].sum().reset_index().round(1)
        else:
            # Create empty return DataFrame with required columns if no return data
            columns = ['xitem'] + return_fields
            df_return = pd.DataFrame(columns=columns)
            # Fill with appropriate data types
            for col in return_fields:
                df_return[col] = df_return[col].astype('float64')
        
        # Merge sales and returns
        merge_fields = ['xitem'] + return_fields
        df_final = df_sales.merge(df_return[merge_fields], on=['xitem'], how='left').fillna(0)
        
        # Calculate final sales and cost
        df_final['final_sales'] = df_final[sales_field] - df_final['totamt']
        
        if include_returnvalue:
            df_final['final_cost'] = df_final['totalvalue'] + df_final['returnvalue']
            drop_fields = [sales_field, 'totamt', 'returnvalue', 'totalvalue']
        else:
            df_final['final_cost'] = df_final['totalvalue']
            drop_fields = [sales_field, 'totamt', 'totalvalue']
        
        df_final = df_final.drop(drop_fields, axis=1)
        
        # Calculate profit metrics
        df_final['Gross_Profit'] = df_final['final_sales'] + df_final['final_cost']
        df_final['Profit_Ratio'] = (df_final['Gross_Profit'] / df_final['final_sales']) * 100
        df_final = df_final.sort_values(by=['Profit_Ratio']).reset_index(drop=True)
        
        # Add totals row
        df_final.loc[len(df_final.index),:] = df_final.sum(axis=0, numeric_only=True)
        df_final.at[len(df_final.index)-1, 'xdesc'] = 'Total_Item_Profit'
        
        # Create summary
        summary = df_final.tail(1).drop('xitem', axis=1)
        summary['Profit_Ratio'] = (summary['Gross_Profit'] / summary['final_sales']) * 100
        summary = summary.to_dict('records')
        
        return {business_name: summary[0]}, df_final
        
    except Exception as e:
        print(f"Error processing {business_name}: {e}")
        return {}, pd.DataFrame()

# Date range setup
COGS_zepto = '04010020'
COGS = '04010020'

end_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
start_date = (datetime.now() - timedelta(days=33)).strftime('%Y-%m-%d')

print(start_date, end_date)
main_data_dict = {}

# Business configurations
business_configs = [
    {
        'zid': ZID_HMBR,
        'name': 'HMBR',
        'use_xdtwotax': False,
        'include_returnvalue': True
    },
    {
        'zid': ZID_GI,
        'name': 'GI Corporation',
        'use_xdtwotax': False,
        'include_returnvalue': True
    },
    {
        'zid': ZID_ZEPTO,
        'name': 'Zepto',
        'use_xdtwotax': True,  # Different: uses xdtwotax instead of xlineamt
        'include_returnvalue': True
    },
    {
        'zid': ZID_HMBR_ONLINE,
        'name': 'hmbr_online_shop',
        'use_xdtwotax': False,
        'include_returnvalue': False  # Different: doesn't include returnvalue in final_cost
    },
    {
        'zid': ZID_PACKAGING,
        'name': 'Packaging',
        'use_xdtwotax': False,
        'include_returnvalue': False  # Different: doesn't include returnvalue in final_cost
    }
]

# Process all businesses using the unified function
business_dataframes = {}  # Store DataFrames for Excel export
for config in business_configs:
    result, df_final = process_business_data(
        zid=config['zid'],
        business_name=config['name'],
        start_date=start_date,
        end_date=end_date,
        use_xdtwotax=config['use_xdtwotax'],
        include_returnvalue=config['include_returnvalue']
    )
    main_data_dict.update(result)
    business_dataframes[config['name']] = df_final

print(main_data_dict)

# ─────────────────────────────────────────────────────────────────────
# 📊 Excel Export - Combine separate sheet of every df_final
# ─────────────────────────────────────────────────────────────────────
sheet_name_mapping = {
    'HMBR': 'HMBR',
    'GI Corporation': 'GI Corp', 
    'Zepto': 'Zepto',
    'hmbr_online_shop': 'HMBR_Online_Shop',
    'Packaging': 'Packaging'
}

with pd.ExcelWriter('item_wise_profit.xlsx') as writer:
    for business_name, df_final in business_dataframes.items():
        if not df_final.empty:
            sheet_name = sheet_name_mapping.get(business_name, business_name)
            df_final.to_excel(writer, sheet_name=sheet_name, index=False)

# ─────────────────────────────────────────────────────────────────────
# 📊 Create Summary DataFrame for Email
# ─────────────────────────────────────────────────────────────────────
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
    })

df = pd.DataFrame(data_rows)

body_text = f"""
Dear Sir,

Please find the 30-day item-wise profit & loss report for 6 businesses.

Period: {start_date} to {end_date}


Full details in attachment. \n
"""

# ─────────────────────────────────────────────────────────────────────
# 📬 Send Email
# ─────────────────────────────────────────────────────────────────────
try:
    recipients = get_email_recipients(os.path.splitext(os.path.basename(__file__))[0])
    recipients = ["ithmbrbd@gmail.com"]
    print(f"📬 Recipients: {recipients}")
except Exception as e:
    print(f"⚠️ Fallback: {e}")
    recipients = ["ithmbrbd@gmail.com"]

subject = f"HM_16 Item-Wise Profit Report – {start_date} to {end_date}"
# Send email with HTML table
send_mail(
    recipient=["ithmbrbd@gmail.com"],
    subject=f"HM_16 Daily Item-Wise Profit Summary {start_date} to {end_date}",
    bodyText="Please find the item-wise profit summary below:",
    attachment=['item_wise_profit.xlsx'],
    html_body=[(df, f"Item-Wise Profit Summary Report {start_date} to {end_date}")]
)

engine.dispose()
print("✅ HM_16 completed successfully.")