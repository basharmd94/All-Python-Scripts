# HM_16 Item-Wise Profit Report - Refactored V3

## What Changed
- **Removed code duplication**: 5 separate business blocks → 1 unified function
- **Fixed bugs**: Empty return data error, pandas warnings
- **Same output**: Excel + email functionality preserved

## Business Logic
| Business | Sales Field | Include Returns |
|----------|-------------|----------------|
| HMBR, GI Corp | `xlineamt` | ✅ Yes |
| Zepto | `xdtwotax` | ✅ Yes |
| HMBR Online, Packaging | `xlineamt` | ❌ No |

## Key Improvements
```python
# Before: 150+ lines of duplicated code
df_sales_1 = get_sales_COGS(ZID_HMBR,...)
# 50+ lines for HMBR
# 50+ lines for GI Corp  
# 50+ lines for Zepto...

# After: Configuration-driven
business_configs = [
    {'zid': ZID_HMBR, 'name': 'HMBR', 'use_xdtwotax': False, 'include_returnvalue': True},
    {'zid': ZID_ZEPTO, 'name': 'Zepto', 'use_xdtwotax': True, 'include_returnvalue': True},
]
for config in business_configs:
    result, df_final = process_business_data(**config)
```

## Benefits
- ✅ 90% less duplicate code
- ✅ Easy to maintain/update
- ✅ Better error handling
- ✅ Same Excel/email output
- ✅ Easy to add new businesses

## Bug Fixes
- Fixed `"['totamt'] not in index"` error for businesses with no returns
- Fixed pandas `FutureWarning` for groupby syntax
- Better exception handling