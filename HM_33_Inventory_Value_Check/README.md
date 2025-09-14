# HM_32_Inventory_Value_Check

**Version:** 1.0.0  
**Last Updated:** January 2025

## 📋 Overview

The HM_32_Inventory_Value_Check script is an automated reporting system that identifies inventory items with negative values (less than -200) across multiple business units. This tool helps maintain accurate inventory records by flagging items that require value corrections.

## 🎯 Purpose

This script monitors inventory values across six different business units and generates reports for items that have negative inventory values exceeding -200. It helps identify discrepancies that may require manual intervention to update Manufacturing Orders (MO), Transfer Orders (TO), or Warehouse records.

## 🏢 Business Units Covered

The script monitors inventory across the following business units:

- **Zepto Chemicals** - Chemical business operations
- **GI Corporation** - Corporate operations
- **GULSHAN TRADING** - Trading operations
- **Sales Warehouse Online Shop** - Online retail operations
- **HMBR Grocery Shop** - Grocery retail operations
- **Gulshan Packaging** - Packaging operations

## 🧠 Logic Flow

1. **Environment Setup**
   - Load environment variables and database configuration
   - Initialize database connection using SQLAlchemy
   - Configure pandas display settings

2. **Data Extraction**
   - Query the `imtrn` (Inventory Transaction) table for each business unit
   - Filter items with total inventory value ≤ -200
   - Group results by item code and warehouse

3. **Report Generation**
   - Create Excel file with separate sheets for each business unit
   - Generate HTML sections for email formatting

4. **Email Distribution**
   - Send automated email with Excel attachment
   - Include HTML-formatted report sections

## 🗃️ Database Tables Used

### Primary Table: `imtrn` (Inventory Transaction)
- **xitem**: Item code
- **xwh**: Warehouse code
- **xqty**: Quantity
- **xsign**: Sign multiplier (+1 or -1)
- **xval**: Value
- **zid**: Business unit identifier

## 📊 Output Files

### Excel Report: `inventory_value_check.xlsx`
Contains separate sheets for each business unit with the following columns:
- **itemcode**: Item identifier
- **warehouse**: Warehouse location
- **qty**: Current quantity
- **totalvalue**: Total inventory value

## 📬 Email Configuration

- **Recipients**: ithmbrbd@gmail.com
- **Subject**: HM_33: Inventory Item's Value Need Correction
- **Attachment**: inventory_value_check.xlsx
- **Format**: HTML email with embedded data tables

## 🔧 Technical Requirements

### Dependencies
- Python 3.x
- pandas
- sqlalchemy
- python-dotenv
- openpyxl (for Excel file generation)

### Environment Variables
The script requires the following environment variables:
- `ZID_GULSHAN_TRADING`
- `ZID_GI`
- `ZID_ZEPTO_CHEMICALS`
- `ZID_HMBR_ONLINE_SHOP`
- `ZID_HMBR_GROCERY`
- `ZID_GULSHAN_PACKAGING`
- `DATABASE_URL`

### Configuration Files
- `mail.py` - Email functionality
- `project_config.py` - Database configuration

## 🚀 Usage

1. Ensure all environment variables are properly configured
2. Run the script: `python HM_32_Inventory_Value_Check.py`
3. The script will:
   - Generate Excel report
   - Send email notification
   - Display completion status

## ⚠️ Important Notes

- Items with inventory values ≤ -200 are flagged for review
- Manual intervention may be required to update MO/TO/Warehouse records
- The script runs across all configured business units automatically
- Database connections are properly disposed after execution

## 🔍 Query Logic

The core query identifies problematic inventory items by:
1. Summing quantity and value transactions per item/warehouse
2. Applying sign multipliers to handle both positive and negative transactions
3. Filtering for total values ≤ -200
4. Ordering by total value (most negative first)

## 📈 Monitoring

The script provides console output for:
- Excel file generation confirmation
- Email recipient information
- Email sending status
- Process completion notification

## 🛠️ Maintenance

- Regular review of flagged items is recommended
- Threshold value (-200) can be adjusted based on business requirements
- Email recipients can be modified in the script configuration
- Additional business units can be added by updating the ZID variables and mapping
