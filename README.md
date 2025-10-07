# End-to-End-Data-Engineering-ML-Platform-for-Olist-E-commerce-Analytics-

**🎯 Project Objective:** This project aims at consolidating all of my data capabilities from the very first step of ingestion, ETL processing for Data Warehousing, to Data Marts for Analysis and Feature Engineering for Machine Learning. 

**🧰 Methodology:** It utilises dbt (data build tool) for version-control of transformation in BigQuries' scalable and serverless architecture. The analytics-ready version from this medallion Data Warehouse structure will then be used for Analytical Queries as well as BI insights in PowerBI. The Workench instance in VertexAI will also be used to train Machine Learning models to capture Customer Churn and Lifetime value (LTV) Prediction, whose performance will be tracked in MLFlow. Ultimately, the whole end-to-end process will be managed by Dagster.

## I. Data Ingestion & Data Warehousing
The data was taken from **Brazilian E-Commerce Public Dataset by Olist** then uploaded by raw into Google Cloud Storage data lake.
Medallion architecture is selected for this Data Warehouse build due to its reliability and data quality enhanced progressively through 3 layers of raw, cleansing, transformation, standardization and business analytics.  

<img width="2672" height="1653" alt="medallion_architecture" src="https://github.com/user-attachments/assets/27b282ce-db57-4b5e-a679-52afa9776d9b" />

### 1. Bronze Layer
This layer consists of 9 raw tables ingested directly from Cloud Storage. This star schema with dim tables surrounding the order_items fact one could potentially increase the cost for JOIN tasks considerably. This seems negligible for a small dataset of 126.19MB; in reality however, this can become bottleneck when the orderdata scales up day by day.

### 2. Silver & Gold Layer
To resolve this issue, the BigQuery ARRAY is utilized to leverage a nested & denormalized structure followed by each order_id. This allows consolidating all data columns into one while still able to avoid data repetation and redundancy. In particular, 3 arrays were created to specify details in each order of order_items_array, payment_array & sellers_array.



