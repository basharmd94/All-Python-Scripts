# === Import Required Libraries ===
import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from dotenv import load_dotenv
import warnings

# === Load Environment & Configuration ===
load_dotenv()  # Load environment variables from .env file

# Business Unit Settings
ZID_ZEPTO = int(os.environ["ZID_ZEPTO_CHEMICALS"])  # Zepto Chemicals business unit ID

# Project Settings
PROJECT_ZEPTO = os.environ["PROJECT_100005"]  # Project code for Zepto operations

# === Setup Project Path ===
PROJECT_ROOT = os.path.dirname(os.getcwd())  # Get parent directory of current working directory
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)  # Add project root to Python path for imports

# === Import Custom Modules ===
from mail import send_mail, get_email_recipients  # Email functionality
from project_config import DATABASE_URL  # Database connection string

# === Database & Pandas Configuration ===
engine = create_engine(DATABASE_URL)  # Create SQLAlchemy database engine
warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)  # Suppress pandas dtype warnings
pd.set_option('display.float_format', '{:.2f}'.format)  # Set pandas to display floats with 2 decimal places


def get_sales_COGS(zid,start_date,end_date):
    """
    Retrieve sales data with COGS (Cost of Goods Sold) information for a specific business unit and date range.
    
    Args:
        zid: Business unit ID
        start_date: Start date for data retrieval (YYYY-MM-DD)
        end_date: End date for data retrieval (YYYY-MM-DD)
    
    Returns:
        DataFrame containing sales transactions with item details, quantities, values, and tax information
    """
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
    """
    Retrieve return/sales return data for a specific business unit and date range.
    
    Args:
        zid: Business unit ID
        start_date: Start date for data retrieval (YYYY-MM-DD)
        end_date: End date for data retrieval (YYYY-MM-DD)
    
    Returns:
        DataFrame containing return transactions with item details, quantities, and return values
    """
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
    """
    Retrieve General Ledger (GL) details for Income and Expenditure accounts, excluding COGS account.
    
    Args:
        zid: Business unit ID
        COGS: Cost of Goods Sold account code to exclude from results
        start_date: Start date for data retrieval (YYYY-MM-DD)
        end_date: End date for data retrieval (YYYY-MM-DD)
    
    Returns:
        DataFrame containing aggregated GL amounts by account type (Income/Expenditure)
    """
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
    """
    Retrieve General Ledger details for Zepto business unit, excluding both COGS and MRP accounts.
    
    Args:
        zid: Business unit ID
        COGS: Cost of Goods Sold account code to exclude
        MRP: MRP account code to exclude
        start_date: Start date for data retrieval (YYYY-MM-DD)
        end_date: End date for data retrieval (YYYY-MM-DD)
    
    Returns:
        DataFrame containing aggregated GL amounts by account type (Income/Expenditure)
    """
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
    """
    Retrieve General Ledger details for a specific project, excluding COGS account.
    
    Args:
        zid: Business unit ID
        project: Project code to filter GL transactions
        start_date: Start date for data retrieval (YYYY-MM-DD)
        end_date: End date for data retrieval (YYYY-MM-DD)
        COGS: Cost of Goods Sold account code to exclude
    
    Returns:
        DataFrame containing aggregated GL amounts by account type (Income/Expenditure) for the project
    """
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


# === Configuration Constants ===
COGS_zepto = '04010020'  # Cost of Goods Sold account code for Zepto business
COGS = '04010020'  # General Cost of Goods Sold account code

# === Date Range Configuration ===
# Set date range: 2 days ago to 33 days ago (31-day reporting period)
end_date = (datetime.now() - timedelta(days = 2)).strftime('%Y-%m-%d')  # End date: 2 days ago
start_date = (datetime.now() - timedelta(days = 33)).strftime('%Y-%m-%d')  # Start date: 33 days ago

print(f"Processing data from {start_date} to {end_date}")  # Display date range being processed
main_data_dict = {}  # Dictionary to store summary data for each business unit

# === ZEPTO BUSINESS UNIT PROCESSING ===
# Process sales data for Zepto Chemicals business unit
df_sales_5 = get_sales_COGS(ZID_ZEPTO,start_date,end_date)  # Get sales transactions with COGS data
df_sales_5 = df_sales_5.groupby(['xitem','xdesc'])['totalvalue','xdtwotax'].sum().reset_index().round(1)  # Group by item and sum values

# Process return data for Zepto business unit
df_return_5 = get_return(ZID_ZEPTO,start_date,end_date)  # Get return transactions
df_return_5 = df_return_5.groupby(['xitem'])['returnvalue','totamt'].sum().reset_index().round(1)  # Group by item and sum return values

# Merge sales and return data
df_final_5 = df_sales_5.merge(df_return_5[['xitem','returnvalue','totamt']],on=['xitem'],how='left').fillna(0)  # Left join and fill NaN with 0

# Calculate final sales amount (sales minus returns)
df_final_5['final_sales'] = df_final_5['xdtwotax'] - df_final_5['totamt']  # Net sales after returns

# Calculate final cost amount (cost plus return value)
df_final_5['final_cost'] = df_final_5['totalvalue'] + df_final_5['returnvalue']  # Total cost including returns

# Clean up unnecessary columns
df_final_5 = df_final_5.drop(['xdtwotax','totamt'],axis=1)  # Remove intermediate sales columns
df_final_5 = df_final_5.drop(['returnvalue','totalvalue'],axis=1)  # Remove intermediate cost columns

# Calculate gross profit and profit ratio
df_final_5['Gross_Profit'] = df_final_5['final_sales'] + df_final_5['final_cost']  # Gross profit calculation
df_final_5['Profit_Ratio'] = (df_final_5['Gross_Profit']/df_final_5['final_sales'])*100  # Profit ratio as percentage

# Sort by profit ratio and add total row
df_final_5 = df_final_5.sort_values(by=['Profit_Ratio']).reset_index(drop=True)  # Sort by profit ratio ascending
df_final_5.loc[len(df_final_5.index),:]=df_final_5.sum(axis=0,numeric_only = True)  # Add total row
df_final_5.at[len(df_final_5.index)-1,'xdesc'] = 'Total_Item_Profit'  # Label total row

# Get GL details for Zepto (excluding COGS and MRP accounts)
df_pl_5 = get_gl_details_zepto(ZID_ZEPTO,COGS_zepto,'07080001',start_date,end_date)  # Get P&L data

# Create summary data for main dictionary
summary_5 = df_final_5.tail(1).drop('xitem',axis=1)  # Get total row without item code
summary_5['Profit_Ratio'] = (summary_5['Gross_Profit']/summary_5['final_sales']) *100  # Recalculate profit ratio for total
summary_5 = summary_5.to_dict('records')  # Convert to dictionary format
df_pl_5 = df_pl_5.to_dict('records')  # Convert GL data to dictionary format

# Add GL data to summary
summary_5[0]['Income_gl'] = df_pl_5[0]['sum']  # Add income from GL
try:
    summary_5[0]['Expenditure_gl'] = df_pl_5[1]['sum']  # Add expenditure from GL
except:
    summary_5[0]['Expenditure_gl'] = 0  # Set to 0 if no expenditure data

# Store summary in main data dictionary
main_data_dict[ZID_ZEPTO] = summary_5[0]  # Store Zepto summary data
main_data_dict[ZID_ZEPTO]['Net']=main_data_dict[ZID_ZEPTO]['Gross_Profit']-main_data_dict[ZID_ZEPTO]['Expenditure_gl']  # Calculate net profit

# Prepare data for Excel export
df_pl_5 = pd.DataFrame(df_pl_5)  # Convert GL data back to DataFrame
df_final_5 = pd.concat([df_final_5, df_pl_5],axis=1)  # Concatenate with main data

# Rename key in main dictionary for better readability
main_data_dict["Zepto"] = main_data_dict.pop(100005)  # Rename numeric key to descriptive name



# === Display Processing Results ===
print("Main Data Dict\n\n", main_data_dict)  # Display summary data for verification

# === Excel Export Section ===
# Export detailed item-wise profit data to Excel with separate sheets for each business unit
with pd.ExcelWriter('item_wise_profit.xlsx') as writer:
    df_final_5.to_excel(writer, sheet_name='Zepto', index=False)  # Export Zepto detailed data to Excel sheet

# === Summary Report Generation ===
# Convert summary dictionary to DataFrame for email report
data_rows = []
for brand, vals in main_data_dict.items():
    data_rows.append({
        'Brand': brand,  # Business unit name
        'Description': vals['xdesc'],  # Description (usually "Total_Item_Profit")
        'Final Sales': f"{vals['final_sales']:,.2f}",  # Net sales amount (formatted with commas)
        'Final Cost': f"{vals['final_cost']:,.2f}",  # Total cost amount (formatted with commas)
        'Gross Profit': f"{vals['Gross_Profit']:,.2f}",  # Gross profit amount (formatted with commas)
        'Profit Ratio (%)': f"{vals['Profit_Ratio']:,.2f}",  # Profit ratio percentage (formatted with commas)
        'Income (GL)': f"{vals['Income_gl']:,.2f}",  # Income from General Ledger (formatted with commas)
        'Expenditure (GL)': f"{vals['Expenditure_gl']:,.2f}",  # Expenditure from General Ledger (formatted with commas)
        'Net': f"{vals['Net']:,.2f}"  # Net profit amount (formatted with commas)
    })

df = pd.DataFrame(data_rows)  # Create DataFrame from summary data

# === Display Final Results ===
print(df_final_5)  # Display detailed item-wise profit data

# === Email Notification ===
# Send email with HTML table containing summary report and Excel attachment
send_mail(
    recipient=["ithmbrbd@gmail.com"],  # Email recipient
    subject=f"Daily Item-Wise Profit Summary {start_date} to {end_date}",  # Email subject with date range
    bodyText="Please find the item-wise profit summary below:",  # Email body text
    attachment=['item_wise_profit.xlsx'],  # Attach Excel file with detailed data
    html_body=[(df, f"Item-Wise Profit Summary Report {start_date} to {end_date}")]  # HTML table with summary data
)