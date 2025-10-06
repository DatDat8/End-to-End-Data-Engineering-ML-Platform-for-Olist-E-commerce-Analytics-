CREATE OR REPLACE TABLE `gold.fct_seller_performance` AS
SELECT
    seller.seller_id,
    seller.seller_state,
    INITCAP(REPLACE(item.product_category, '_',' ')) AS product_category,
    COUNT(t1.order_id) AS total_orders_shipped,
    ROUND(SUM(item.price),2) AS total_revenue_contribution,
    
    -- On-Time Delivery Rate
    ROUND(
        SUM(CASE
            WHEN t1.order_delivered_customer_date <= t1.order_estimated_delivery_date
            THEN 1 ELSE 0
        END) / COUNT(t1.order_id), 4) AS on_time_delivery_rate,

    -- Average Rating
    ROUND(AVG(t1.review_score), 2) AS average_review_score 
    
FROM `silver.denormalized_orders` AS t1,
    UNNEST(t1.sellers_array) AS seller, -- Flatten the seller array to take sellers' info
    UNNEST(t1.order_items_array) AS item -- Flatten the item array to link sellers and items
    -- UNNEST(t1.order_items_array) AS item -- Flatten the item array to link sellers and items
WHERE t1.order_status = 'delivered'
GROUP BY 1,2,3
ORDER BY 1,3,2;
