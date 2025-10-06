CREATE OR REPLACE TABLE `gold.agg_payment_behavior` AS
SELECT
  p.payment_type,
  t1.order_status,
  item.product_category,

  -- BEHAVIOR METRICS
  COUNT(DISTINCT t1.order_id) AS total_dist_orders,
  COUNT(t1.order_id) AS total_orders,
  ROUND(SUM(t1.calculated_order_cost),2) AS total_gmv,
  ROUND(SUM(t1.calculated_order_cost - t1.total_payment_value),2) AS payment_missing,
  ROUND(SUM(p.payment_installments),2) AS total_installments,
  
  -- RISK METRICS
  SUM(CASE WHEN t1.payment_match_flag IN ('Discrepancy', 'Missing Payment') THEN 1 ELSE 0 END) AS mismatch_transactions_count,
  
  ROUND(SAFE_DIVIDE(
      SUM(CASE WHEN t1.payment_match_flag IN ('Discrepancy', 'Missing Payment') THEN 1 ELSE 0 END),
      COUNT(DISTINCT t1.order_id)
  ),4) AS payment_mismatch_rate,
  ROUND(SUM(review_score),2) AS total_review_score
    
FROM silver.denormalized_orders AS t1,
UNNEST(t1.order_items_array) AS item,
UNNEST(t1.payments_array) AS p

GROUP BY 1,2,3
ORDER BY 1,2,mismatch_transactions_count DESC;

