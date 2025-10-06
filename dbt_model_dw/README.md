Welcome to my dbt project for silver & gold transformation for Olist Data Warehouse!

### The Models' Components for Each Medallion Layer:
- Bronze Layer: Schema in yml & Identifier directly from the BigQuery project named 'olist-ecommerce-dw', with dataset named 'bronze'
- Silver Layer: Schema in yml & Model transformation for one materialized table which is denormalized from all bronze tables to leverage ARRAY structure for better JOIN performance.
- Gold Layer: Schema in yml & Model Transformation for 6 materialized analytics-ready tables which are aggregated and dimensions from the single silver table.

Try running the following CLI commands:
- dbt run
- dbt test
For running a specific model transformation:
- dbt run -s agg_sales_kpi.sql