# HM_34 Customer Segmentation Analysis

## Overview
This module provides comprehensive customer segmentation analysis based on payment timeliness, sales performance, collection patterns, and DSO metrics for all business units (GI, Trade, Zepto).

## Scripts

### 1. HM_34_Customer_Segment.py
**Purpose:** Main customer segmentation analysis script
- **Schedule:** Runs on the first day of each month
- **Function:** 
  - Fetches AR transaction data from database
  - Processes voucher types and calculates running balances
  - Computes payment timeliness metrics (days to pay, DSO, etc.)
  - Calculates composite scores based on weighted metrics
  - Generates dynamic customer segments using composite scores
  - Updates database with segment information
  - Generates summary reports (CSV files)

### 2. HM_34_Segment_Email.py
**Purpose:** Daily email notification for customer segmentation alerts
- **Schedule:** Runs every day except Friday at 10 PM
- **Function:**
  - Queries daily orders for all business units
  - Filters customers in critical segments (Critical Watch, High Risk, Warning Zone, Needs Attention)
  - Sends email alerts with HTML tables showing problematic orders
  - Recipients configured via email_list.csv

## Customer Segments
- **Critical Watch** - Highest risk customers
- **High Risk** - Customers requiring immediate attention
- **Warning Zone** - Customers showing concerning patterns
- **Needs Attention** - Customers requiring monitoring
- **Developing** - Customers with potential
- **Stable** - Consistent performers
- **Solid Performer** - Good customers
- **Valued Partner** - Important customers
- **Top Tier** - Premium customers
- **Elite Champion** - Best customers

## Database Updates
The main script updates the `cacus` table with:
- `xtitle` - Customer segment name
- `xfax` - Composite score bin range

## Email Recipients
Configured in `email_list.csv` under `HM_34_Customer_Segment` entry.
