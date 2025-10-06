# End-to-End-Data-Engineering-ML-Platform-for-Olist-E-commerce-Analytics-

**Project Objective:** This project aims at consolidating all of my data capabilities from the very first step of ingestion, ETL processing for Data Warehousing, to Data Marts for Analysis and Feature Engineering for Machine Learning. 
**Methodology:** It utilises dbt (data build tool) for version-control of transformation in BigQuries' scalable and serverless architecture. The analytics-ready version from this medallion Data Warehouse structure will then be used for Analytical Queries as well as BI insights in PowerBI. The Workench instance in VertexAI will also be used to train Machine Learning models to capture Customer Churn and Lifetime value (LTV) Prediction, whose performance will be tracked in MLFlow. Ultimately, the whole end-to-end process will be managed by Dagster.

## I. Data Ingestion & Data Warehousing
The data was taken from **Brazilian E-Commerce Public Dataset by Olist** then ingested by raw into Google Cloud Storage data lake.
