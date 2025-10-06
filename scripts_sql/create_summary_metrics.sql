CREATE OR REPLACE TABLE `gold.agg_sales_kpi` AS
-- WITH summary_metrics AS (
SELECT
  item.product_category,
  t.order_status,
  t.payment_match_flag,
  t.customer_state,
  t.customer_city,
  ROUND(SUM(t.calculated_order_cost),2) AS total_sales,
  COUNT(DISTINCT t.customer_unique_id) AS total_customers,
  COUNT(DISTINCT t.order_id) AS total_orders,
  ROUND(SUM(t.num_early_delivery_days),2) AS total_early_delivery_days,
  SUM(t.is_delivered_on_time) AS total_on_time_delivery,
  SUM(t.review_score) AS review_score
FROM `silver.denormalized_orders` AS t,
  UNNEST(order_items_array) AS item
GROUP BY 1,2,3,4,5;


  -- Extract the first seller ID from the array for single-seller focus
  -- sellers_array[OFFSET(0)].seller_id AS primary_seller_id,