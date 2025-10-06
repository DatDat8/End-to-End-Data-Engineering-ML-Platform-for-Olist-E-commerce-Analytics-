CREATE OR REPLACE TABLE `gold.agg_order_summary` AS 
  SELECT
    order_id,
    order_status,
    DATE(order_purchase_timestamp) AS purchase_date,
    customer_unique_id,
    customer_city,
    customer_state,
    FIRST_VALUE(customer_state) OVER(
      PARTITION BY customer_unique_id 
      ORDER BY order_purchase_timestamp ASC
    ) AS primary_state,
    calculated_order_cost AS order_value, -- Base for AOV
    total_payment_value,
    payment_match_flag,
    
    -- Extract the first seller ID from the array for single-seller focus
    sellers_array[OFFSET(0)].seller_id AS primary_seller_id,
    
    -- Calculate Delivery Metrics
    CASE
        WHEN order_delivered_customer_date <= order_estimated_delivery_date THEN 1
        ELSE 0
    END AS is_delivered_on_time,

    DATE_DIFF(order_delivered_customer_date, order_approved_at, DAY) AS num_delivery_days,
    ROUND(
      DATETIME_DIFF(order_estimated_delivery_date, order_delivered_customer_date, HOUR)/24.0, 
      2) AS num_early_delivery_days,
    
    -- Assume the current date as the most recent day in the dataset
    (SELECT MAX(DATE(order_purchase_timestamp)) FROM `silver.denormalized_orders`) AS current_day,
    -- Calculate Total Items using Array Function
    ARRAY_LENGTH(order_items_array) AS total_items_quantity

FROM `silver.denormalized_orders`
WHERE order_status = 'delivered';