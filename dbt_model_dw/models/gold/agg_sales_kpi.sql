{{
  config(
    materialized='table',
    schema='gold',
    alias='agg_sales_kpi',
    tags=['aggregate', 'sales', 'kpi']
  )
}}

SELECT
    item.product_category,
    t.order_status,
    t.payment_match_flag,
    t.customer_state,
    t.customer_city,
    ROUND(SUM(t.calculated_order_cost), 2) AS total_sales,
    COUNT(DISTINCT t.customer_unique_id) AS total_customers,
    COUNT(DISTINCT t.order_id) AS total_orders,
    
    -- Sum of early delivery days (negative means late)
    ROUND(SUM(t.num_early_delivery_days), 2) AS total_early_delivery_days,
    SUM(t.is_delivered_on_time) AS total_on_time_delivery,
    
    SUM(t.review_score) AS total_review_score,
    CURRENT_TIMESTAMP() as _ingest_ts
    
FROM {{ ref('denormalized_orders') }} AS t,
    UNNEST(order_items_array) AS item
GROUP BY 1, 2, 3, 4, 5
-- ORDER BY clause is typically added in BI tools, but can be added here if needed