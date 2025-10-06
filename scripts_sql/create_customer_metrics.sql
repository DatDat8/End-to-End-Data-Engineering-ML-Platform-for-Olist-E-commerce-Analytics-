CREATE OR REPLACE TABLE `gold.dim_customer_metrics` AS
SELECT
    customer_unique_id,
    primary_state,
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND(SUM(order_value), 2) AS total_spent, -- Monetary value (LTV input)
    MIN(purchase_date) AS first_purchase_date,
    MAX(purchase_date) AS last_purchase_date,
    
    -- Calculate Recency (Days since last purchase)
    DATE_DIFF(CAST('2018-09-03' AS DATE), MAX(purchase_date), DAY) AS recency_days

FROM `gold.agg_order_summary`
WHERE order_status = 'delivered'
GROUP BY 1, 2;

-- SELECT * FROM dim_customer_metrics;
-- SELECT 
--   -- customer_city, 
--   ROUND(AVG(early_deliver_days),2) AS avg_early_deliver_days,
--   ROUND(100*SUM(is_delivered_on_time)/(SELECT COUNT(*) FROM agg_order_summary), 2) AS ptg_delivery_promise_accuracy,
--   MAX(purchase_date),
--   MIN(purchase_date)
-- -- SELECT SUM(order_value)/COUNT(DISTINCT order_id) AS avg_value_per_unit
-- FROM agg_order_summary
-- -- GROUP BY customer_city;



-- SELECT
--     t1.order_id,
--     t1.customer_unique_id,
--     DATE_TRUNC(t1.order_purchase_timestamp, MONTH) AS purchase_month,
--     item.order_item_id,
--     item.product_id,
--     item.product_category_name, -- The column PowerBI will filter on!
--     item.price
-- FROM {{ ref('fct_denormalized_orders') }} AS t1,
-- UNNEST(t1.order_items_array) AS item


