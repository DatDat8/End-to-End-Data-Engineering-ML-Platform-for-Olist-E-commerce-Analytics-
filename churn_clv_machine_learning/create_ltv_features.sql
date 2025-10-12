DECLARE final_cutoff_date DATETIME DEFAULT (SELECT MAX(order_purchase_timestamp) FROM `silver.denormalized_orders`);
DECLARE observation_date DATETIME DEFAULT DATETIME_SUB(final_cutoff_date, INTERVAL 6 MONTH);
DECLARE lookback_date DATETIME DEFAULT DATETIME_SUB(observation_date, INTERVAL 1 YEAR);
CREATE OR REPLACE TABLE gold.agg_ltv_features AS
WITH features_ltv AS (
  SELECT 
    -- customer_unique_id,
    s.seller_id,
    -- F: Frequency (Count of orders in the 1-year lookback window)
    COUNT(order_id) AS total_orders,
    -- M: Monetary (Days Since Last Purchase)
    ROUND(SUM(total_payment_value),2) AS total_spent,
    ROUND(COALESCE(SAFE_DIVIDE(SUM(total_payment_value),COUNT(order_id)), 0),2) AS avg_order_value,
    -- R: Recency (Days Since Last Purchase)
    DATE_DIFF(observation_date, MAX(order_purchase_timestamp), DAY) AS recency_days,
    -- Duration between first and observation date
    DATE_DIFF(observation_date, MIN(order_purchase_timestamp), DAY) AS days_since_first_purchase,
    -- Duration between first & last order
    DATE_DIFF(MAX(order_purchase_timestamp),MIN(order_purchase_timestamp),DAY) AS customer_tenure_days,

    -- Delivery and Fulfillment Experience
    ROUND(AVG(DATE_DIFF(order_estimated_delivery_date, order_delivered_customer_date, DAY)),2) AS avg_delivery_delay,
    ROUND(AVG(is_delivered_on_time), 2) AS pct_ontime_deliveries,
    ROUND(AVG(num_early_delivery_days), 2) AS avg_early_deliver_days,
    ROUND(AVG(DATE_DIFF(order_delivered_customer_date, order_purchase_timestamp, DAY)),2) AS avg_shipping_duration,

    -- Review & Customer Feedback
    ROUND(COALESCE(AVG(review_score),0),2) AS avg_review_score,
    COUNT(review_score) AS review_count,
    SUM(CASE WHEN review_score >=4 THEN 1 ELSE 0 END)/COUNT(*) AS pct_positive_reviews,
    ROUND(COALESCE(AVG(DATE_DIFF(review_cre_date,review_ans_datetime, DAY)),0),2) AS avg_response_time,

    -- Payment Behavior
    ROUND(COUNT(CASE WHEN p.payment_type='Credit Card' THEN 1 ELSE 0 END)/COUNT(*),2) AS credit_pay_rate,
    ROUND(AVG(p.payment_installments),2) AS avg_payment_installments,
    ROUND(COUNT(CASE WHEN payment_match_flag='Match' THEN 1 ELSE 0 END)/COUNT(*),2) payment_match_rate,

    -- Product & Category Affinity
    COUNT(DISTINCT i.product_id) AS unique_products_bought,
    COUNT(DISTINCT i.product_category) AS unique_categories_bought,
    ROUND(AVG(i.price)) AS avg_product_price,
    ROUND(AVG(i.freight_value)) AS avg_freight_value,

    -- Temporal and Seasonality Factors
    EXTRACT(MONTH FROM MAX(order_purchase_timestamp)) AS last_purchase_month,
    COALESCE(SUM(CASE WHEN EXTRACT(MONTH FROM order_purchase_timestamp) IN (11,12) THEN 1 END)/COUNT(*),0) AS pct_orders_peak_season

  FROM `silver.denormalized_orders` AS o,
    UNNEST(payments_array) AS p,
    UNNEST(order_items_array) AS i,
    UNNEST(sellers_array) AS s
  WHERE order_status = "delivered"
    AND order_purchase_timestamp < observation_date
    AND order_purchase_timestamp > lookback_date
  GROUP BY 1
),

target_ltv AS (
  SELECT
    s.seller_id,
    ROUND(SUM(total_payment_value),2) AS ltv_6m
  FROM `silver.denormalized_orders` AS o,
    UNNEST(sellers_array) AS s
  WHERE order_status = "delivered" 
    AND order_purchase_timestamp > observation_date
    -- AND order_purchase_timestamp < final_cutoff_date
  GROUP BY 1
)
SELECT 
  f.*, COALESCE(t.ltv_6m,0) AS ltv_6m
FROM features_ltv AS f
LEFT JOIN target_ltv AS t USING(seller_id);
