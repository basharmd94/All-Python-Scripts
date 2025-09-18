# HM_25_Zepto_Overall_Details

## Purpose
This script generates a comprehensive business report for Zepto Chemicals. It includes customer sales, accounts receivable (AR), accounts payable (AP), item sales, and item purchases, summarized by various business units (Zepto, HMBR, Fixit, E-Commerce, etc.).

## Affected Business
- **Zepto Chemicals (ZID=100005)**
- Data Source: PostgreSQL (localhost:5432/da)

## Period
The report covers the last 6 months of data, with a dynamic month range based on the current date.

## Output
- `HM_25_Zepto_Overall_Details.xlsx`: A multi-sheet Excel file containing detailed reports.
- `index.html`: An HTML summary for email embedding.
- An email with the full HTML body and the Excel attachment.

## Email Details
- Sent via raw SMTP.
- The HTML body includes 5 tables with red headers.
- Recipients: `ithmbrbd@gmail.com` (dynamic recipients based on `get_email_recipients` function).
- Attachment: `HM_25_Zepto_Overall_Details.xlsx`

## Enhancements
- Utilizes `project_config.DATABASE_URL`.
- Output Excel file is prefixed with `HM_25`.
- Improved comments and documentation within the script.
- No changes to the core logic or flow compared to the original.
- Includes one-line cell documentation at the end.

## Setup and Configuration
1.  **Environment Variables**: Ensure `ZID_ZEPTO_CHEMICALS` is set in your `.env` file.
2.  **Database**: The script connects to a PostgreSQL database using `DATABASE_URL` from `project_config.py`.
3.  **Dependencies**:
    - `pandas`
    - `numpy`
    - `psycopg2`
    - `sqlalchemy`
    - `python-dotenv`
    - `dateutil` (for `relativedelta`)

## How to Run
Execute the `HM_25_Zepto_Overall_Details.py` script. It will:
1.  Fetch employee, customer, sales, return, accounts receivable, accounts payable, and purchase data from the database.
2.  Process and summarize the data by various business units and time periods.
3.  Generate an Excel report.
4.  Generate an HTML summary.
5.  Send an email with the report and HTML summary to the configured recipients.