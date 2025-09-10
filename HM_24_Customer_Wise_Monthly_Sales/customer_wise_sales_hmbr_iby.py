# main_report.py

from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import openpyxl
from mail import send_mail  # Importing your custom send_mail function


# --------------------------
# Database Functions
# --------------------------

def get_sales(engine, zid, start_date):
    query = """
        SELECT opdor.xdate, opdor.xdiv, opdor.xcus, SUM(opdor.xdtwotax) AS total
        FROM opdor
        WHERE opdor.zid = %(zid)s
          AND opdor.xdate >= %(start_date)s
        GROUP BY opdor.xdate, opdor.xcus, opdor.xdiv
        ORDER BY opdor.xdate ASC;
    """
    df = pd.read_sql(query, con=engine, params={"zid": zid, "start_date": start_date})
    return df


def get_cacus(engine, zid):
    query = """
        SELECT cacus.xcus, cacus.xshort, cacus.xmobile, cacus.xadd1, cacus.xadd2
        FROM cacus
        WHERE cacus.zid = %(zid)s;
    """
    df = pd.read_sql(query, con=engine, params={"zid": zid})
    return df


# --------------------------
# Report Generation Function
# --------------------------

def generate_report(zid, business_name, start_date, attachment_list):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')

    # Fetch data
    sales_df = get_sales(engine, zid, start_date)
    customer_df = get_cacus(engine, zid)

    if sales_df.empty:
        print(f"No sales data found for {business_name} (zid={zid})")
        return

    # Add year and month columns
    sales_df['xdate'] = pd.to_datetime(sales_df['xdate'])
    sales_df['year'] = sales_df['xdate'].dt.year
    sales_df['month'] = sales_df['xdate'].dt.month

    # Group by relevant fields
    grouped_df = sales_df.iloc[:, 1:].copy()
    grouped_df = grouped_df.groupby(['xdiv', 'xcus', 'month', 'year']).sum(numeric_only=False).reset_index()

    # Add grand total per customer
    grouped_df['grand_total'] = grouped_df.groupby('xcus')['total'].transform('sum')

    # Pivot to monthly format
    pivot_df = pd.pivot_table(grouped_df, values='total', index=['xcus', 'year', 'xdiv'], columns='month', aggfunc='sum').reset_index()
    pivot_df['grand_total'] = pivot_df.iloc[:, 3:].sum(axis=1)

    # Merge with customer info
    final_df = pd.merge(pivot_df, customer_df, on='xcus', how='left')

    # Rename months
    month_rename = {
        1: 'January',
        2: 'February',
        3: 'March',
        4: 'April',
        5: 'May',
        6: 'June',
        7: 'July',
        8: 'August',
        9: 'September',
        10: 'October',
        11: 'November',
        12: 'December'
    }

    final_df.rename(columns=month_rename, inplace=True)

    # Reorder columns
    cols = ['xcus', 'year', 'xshort', 'xmobile', 'xdiv', 'xadd2'] + list(month_rename.values()) + ['grand_total']
    final_df = final_df[cols]

    # Save to Excel
    filename = f"{business_name.replace(' ', '_')}_monthly_sales_customer_wise.xlsx"
    final_df.to_excel(filename, index=False, engine='openpyxl')
    print(f"Saved report for {business_name} to {filename}")

    # Append to attachment list
    attachment_list.append(filename)


# --------------------------
# Main Execution
# --------------------------

if __name__ == "__main__":
    # Shared recipients for combined report
    shared_recipients = ['ithmbrbd@gmail.com', 'saleshmbrbd@gmail.com' ]

    # List to collect file paths
    attachments = []

    # Define businesses
    businesses = [
        {"zid": 100001, "name": "HMBR"},
        {"zid": 100000, "name": "GI-Corp"}
    ]

    # Set time range
    end_date = datetime.today().strftime('%Y-%m-%d')
    start_date = (datetime.today() - relativedelta(years=1)).strftime('%Y-%m-%d')

    # Generate reports and collect attachments
    for business in businesses:
        generate_report(business["zid"], business["name"], start_date, attachments)

    # Send one email with both reports
    if attachments:
        subject = "Monthly Sales Reports Customer Wise- HMBR & GI-Corp"
        body_text = "Please find attached the monthly sales reports Customer Wise for HMBR and GI-Corp."

        send_mail(
            subject=subject,
            bodyText=body_text,
            attachment=attachments,
            recipient=shared_recipients
        )
        print(f"✅ Email sent with attachments: {', '.join(attachments)}")
    else:
        print("❌ No reports were generated. Email not sent.")