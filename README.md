# End-to-End-Data-Engineering-ML-Platform-for-Olist-E-commerce-Analytics-


**🎯 Project Objective:** This project aims at consolidating all of my data capabilities from the very first step of ingestion, ETL processing for Data Warehousing, to Data Marts for Analysis and Feature Engineering for Machine Learning. 

**🏹 Methodology:** It utilises dbt (data build tool) for version-control of transformation in BigQuries' scalable and serverless architecture. The analytics-ready version from this medallion Data Warehouse structure will then be used for Analytical Queries as well as BI insights in PowerBI. The Workench instance in VertexAI will also be used to train Machine Learning models to capture Customer Churn and Lifetime value (LTV) Prediction, whose performance will be tracked in MLFlow. Ultimately, the whole end-to-end process will be managed by Dagster.

## I. Data Ingestion & Data Warehousing 🗃️
The data was taken from **Brazilian E-Commerce Public Dataset by Olist** then uploaded by raw into Google Cloud Storage data lake.
Medallion architecture is selected for this Data Warehouse build due to its reliability and data quality enhanced progressively through 3 layers of raw, cleansing, transformation, standardization and business analytics.

<img width="2956" height="1668" alt="medallion_architecture" src="https://github.com/user-attachments/assets/e976fe17-5e9e-43ca-b733-f0332c393fc6" />

### 1. Bronze Layer 🥉
This layer consists of 9 raw tables ingested directly from Cloud Storage. This star schema with dim tables surrounding the order_items fact one could potentially increase the cost for JOIN tasks considerably. This seems negligible for a small dataset of 126.19MB; in reality however, this can become bottleneck when the orderdata scales up day by day.

### 2. Silver Layer 🥈
To resolve this issue, the BigQuery ARRAY is utilized to leverage a nested & denormalized structure followed by each order_id. This allows consolidating all data columns into one while still able to avoid data repetation and redundancy. In particular, 3 arrays were created to specify details in each order of order_items_array, payment_array & sellers_array.

<img width="2434" height="854" alt="table_ci_benmark" src="https://github.com/user-attachments/assets/453f88ff-8de6-41e3-8593-436ceca8d064" />

To prove this enhanced performance, a benchmark was conducted to measure the amount of Billed Bytes and Processed Bytes on Gold Tables between the traditional JOIN jobs from Star-Schema Bronze tables and the ARRAY silver one. The result clearly shows a significant 75% of cost saved by using this Denormalized structure.
For detailed Job test, please enter ./test/benchmark_performance_test.

### 3. Gold Layer 🥇
To serve SQL analytics & business intelligence on PowerBI purposes, the last layers contain cleaned, aggregated and standardized tables including:
  * agg_sales_kpi: Average Order Value (AOV), Average Revenue per Unit (ARPU), Overall Performance of Sales, Orders & Delivery Over Product Categories, Order Statuses & Customer States
  * fct_customer_ltv: Customer Lifetime Value (LTV) - total revenue gained through a customer.
  * fct_seller_performance: Seller Performance Tracking.
  * agg_cohort_retention: Cohort Analysis for Customer Retention Rate on a 6-month period.
  * agg_payment_behavoir: Payment Behavior & Installment Type Risk
  * agg_churn_features: Feature engineering for Churn Prediction model training 

The transformation into silver and gold layer was implemented using dbt which allows robust development, seamless collaboration and version-control deployment for ETL process on cloud. Please enter to ./dbt_model_dw for its detailed development as well as schemas for silver & gold tables. 

## II. Customer Churn Classification 🛟
The agg_churn_features containing engineering features from customers' behavior in gold layer is then used to predict churn rate in a 6-month period. A churned customer is defined when he/she doesn't place any orders in this 3-month period. To inspect the causes of this, features are generated mainly based on RFM analysis for using the observation of 1-year backward:
  * Recency: Days since last purchase/activity in the tracked window
  * Frequency: Count of of orders in the 1-year lookback window 
  * Monetary: Sum of LTV in 1-year before the observation date.

As can be seen from the feature importance figure, the most impactful features on churn appears mainly on those RFM ones Frequency (num_orders), Recency (days_since_last_purchase) and Monetary (ltv_180d).

<img width="1859" height="543" alt="confusion_matrix" src="https://github.com/user-attachments/assets/63f0fdc1-74e9-4734-83e3-985bb037acd3" />

* As can be seen, the Random Forest model has the highest F1-Score (0.765) and the highest AUC ROC (0.899). This indicates that it provides the best balance of Precision and Recall, and is also the most effective model at correctly separating the positive and negative classes.
* The Random Forest model also achieved the highest scores for both Precision (0.783) and Recall (0.747). This means it is the best at minimizing both false positive (FP) and false negative (FN) predictions simultaneously.
* Both the Random Forest (0.845) and XGBoost (0.844) models show a strong, nearly identical performance on the PR ROC curve. This suggests that both ensemble methods are robust when dealing with a potentially imbalanced dataset, significantly outperforming the single Decision Tree (0.773).

<img width="600" height="420" alt="roc_metrics" src="https://github.com/user-attachments/assets/6af9915d-efbc-484d-a698-93f357c33653" />

* Random Forest give significant scores over 8 highest scores, while XGBoost has almost idential scores over the majority of the features.
* Moreover, both models consider recency_days as the most important feature and payment_match_rate and credit_pay_rate as the most negligible ones. This demonstrates the importance of purchase time-related columns and unimportance of payment behavior ones. This results in a significance of recency, total_orders(frequency), last_purchase_month, review_count, and days_since_first_purchase in predicting churn of this e-commerce platform.

<img width="800" height="576" alt="fi" src="https://github.com/user-attachments/assets/4656c200-845b-4bec-a0f0-3c9f6cc862ff" />

* The metrics show a significant improvement in both recall (reached high sensitivity of 0.808) and f1-score (higher harmonic mean of recall and precision of 0.782) of Random Forest after top-5 features selection. 
* XGBoost can nearly obtain the same performance using up to 8 features, which demonstrates the even weak scores it gives cross the majority of features, as can be shown in the previous plot. 
* The lower features to be extracted, the lower the cost for training session. This highlights the advantage and performance of Random Forest, and that makes this model officially chosen to resolve Churn Prediction.


**🚀 Project Progress:** This project is ongoing and there're still tasks to be done:
  * Design & Implement 3 PowerBI dashboards titled Total Sales Performance, CLT & Cohort Analysis and Sellers Performance & Payment Behavior
  * Compare classification models for Churn Classification and CLV Prediction, with model lifetime tracking using MLflow
  * Automate daily Ingestion jobs using BigQuery API, orchestrate the whole workflow by Dagster
