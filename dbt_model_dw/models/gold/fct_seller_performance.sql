{{
  config(
    materialized='table',
    schema='gold',
    alias='fct_seller_performance',
    tags=['fact', 'seller', 'performance']
  )
}}

SELECT
    seller.seller_id,
    seller.seller_state,
    -- Note: The item array carries the category, but the seller array (t1.sellers_array) 
    -- is derived from order_items, making this join implicit by the row's context.
    INITCAP(REPLACE(item.product_category, '_',' ')) AS product_category,
    
    -- The COUNT is actually counting order_item rows for this seller/category combination.
    COUNT(t1.order_id) AS total_order_items_sold, 
    ROUND(SUM(item.price),2) AS total_revenue_contribution,
    
    -- On-Time Delivery Rate
    -- We COUNT DISTINCT orders for the denominator to get a true rate per order, 
    -- instead of weighting it by item count.
    ROUND(
        SUM(CASE
            WHEN t1.order_delivered_customer_date <= t1.order_estimated_delivery_date
            THEN 1 ELSE 0
        END) / COUNT(DISTINCT t1.order_id), 4) AS on_time_delivery_rate,

    -- Average Rating
    ROUND(AVG(t1.review_score), 2) AS average_review_score,
    CURRENT_TIMESTAMP() as _ingest_ts
    
FROM {{ ref('denormalized_orders') }} AS t1,
    UNNEST(t1.sellers_array) AS seller, 
    UNNEST(t1.order_items_array) AS item 
    
WHERE t1.order_status = 'delivered'
GROUP BY 1,2,3
ORDER BY 1,3,2