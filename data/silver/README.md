## Silver Dataset Contract — fact_trade_clean

### Grain
One row represents:
Monthly trade flow for a given reporter, partner, and product.

### Required Columns

| column_name | type     | description |
|------------|----------|-------------| 
| reporter   | string   | Reporting country code |
| partner    | string   | Partner country code |
| product_nc | string   | HS product code |
| flow       | string   | Import or export indicator |
| period     | date     | Month of record (YYYY-MM-01) |
| value_eur  | float    | Trade value in euros |
| quantity_kg| float    | Quantity in kilograms |


### Constraints

- No nulls in: reporter, partner, product_nc, period
- period always first day of month 
- One UNIQUE row per (reporter, partner, product_nc, flow, period)
- value_eur >= 0
- quantity_kg >= 0

### Format

- Parquet
- Single dataset (can be partitioned by year if scaled later)
```
data/silver/
    fact_trade_clean.parquet
```
*partitioned by year (optional)*
```
data/silver/
  year=2002/
    part-001.parquet
  year=2003/
    part-002.parquet
  year=2004/
    part-003.parquet
```