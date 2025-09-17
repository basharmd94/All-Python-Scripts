# 🚀 HM_01_Acct_Rec_Pay.py – Accounts Receivable & Payable Report

Automated financial reporting script that generates **Accounts Receivable (AR)** and **Accounts Payable (AP)** summaries for multiple HMBR companies.

## ✅ Features

- 📊 **AR Report**: Compares receivables across 2 months (current + previous) for each customer, grouped by city.
- 💼 **AP Report**: Lists payable amounts per supplier for the current month.
- 📁 **Excel Output**: 
  - `accountsReceivable.xlsx` — with 24-month column headers & automatic sum row.
  - `accountsPayable.xlsx` — per company supplier listing.
- 📧 **Email Delivery**: Sends HTML summary tables + Excel attachments via internal mail utility.
- 🔐 **Secure**: Uses parameterized SQL queries to prevent injection.
- 🧩 **Dynamic**: Loads company names & IDs from `.env` (e.g., `PROJECT_100000=GI .`).
- 🔄 **Reusable**: Built for PostgreSQL, easily adaptable to other GL structures.


