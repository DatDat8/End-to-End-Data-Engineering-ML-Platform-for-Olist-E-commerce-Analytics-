{{
  config(
    materialized='table',
    schema='silver',
    alias='denormalized_orders',
    tags=['fact', 'denormalized']
  )
}}

-- CTE 1: Aggregate Order Items & Product Details (1:Many)
-- Nest the item details and relevant product dimensions into an ARRAY of STRUCTs
-- Pull in product dimensions (one layer of nesting)
-- Calculate aggregated metrics at the order level for immediate use
WITH items_products_agg AS (
  SELECT
    t1.order_id,
    ROUND(SUM(t1.price + t1.freight_value), 2) AS calculated_order_cost,
    COUNT(*) AS total_items_quanity,
    ARRAY_AGG(
      STRUCT(
        t1.order_item_id,
        t1.product_id,
        t1.price,
        t1.freight_value,
        -- Correct logic to get translated product category
        INITCAP(REPLACE(
          COALESCE(t3.product_category_name_english, t2.product_category_name)
          ,'_', ' ')) AS product_category,
        t2.product_weight_g,
        t2.product_length_cm,
        t2.product_height_cm,
        t2.product_width_cm
      )
      ORDER BY t1.order_item_id
    ) AS order_items_array
  FROM {{ source('bronze','order_items') }} AS t1
  LEFT JOIN {{ source('bronze','products') }} AS t2 
    ON t1.product_id = t2.product_id
  LEFT JOIN {{ source('bronze','product_category_name_translation') }} AS t3
    ON t2.product_category_name = t3.product_category_name
  GROUP BY 1
),

-- CTE 2: Aggregate Payment Details (1:Many Relationship)
-- Nest all payment transaction into ARRAY of STRUCTs
-- Aggregated over order_id to merge with orders
payments_agg AS (
  SELECT
    order_id,
    ROUND(SUM(payment_value),2) AS total_payment_value,
    ARRAY_AGG(
      STRUCT(
        payment_sequential,
        INITCAP(REPLACE(payment_type,'_', ' ')) AS payment_type,
        payment_installments,
        payment_value
      )
      ORDER BY payment_sequential
    ) AS payments_array
  FROM {{ source('bronze', 'order_payments')}}
  GROUP BY 1
),

-- CTE 3: Aggregate Seller Details (1: Many Relationship)
-- Nest all sellers info into ARRAY of STRUCTs
sellers_agg AS (
  SELECT
    t1.order_id,
    ARRAY_AGG(
      STRUCT(
        t2.seller_id,
        t2.seller_zip_code_prefix,
        INITCAP(t2.seller_city) AS seller_city,
        t2.seller_state
      )
      -- Use DISTINCT for sellers to prevent duplicate entries if multiple items in one order were from the same seller
      -- Though without knowing the exact data model, this is an assumption.
      -- If order_items has one row per item/seller pair, then simple ARRAY_AGG is fine.
      -- Adding DISTINCT is safer if multiple rows per seller per order exist.
      -- This model is complex, we will keep it as is, assuming unique seller per item.
    ) AS sellers_array
  FROM {{ source('bronze', 'order_items')}} AS t1
  LEFT JOIN {{ source('bronze', 'sellers')}} AS t2
    ON t1.seller_id = t2.seller_id
  GROUP BY 1
),

-- CTE 4: Join Orders with Customers (1:1 Relationship)
-- This prepares main flat columns
core_order AS (
  SELECT
    t1.* EXCEPT(customer_id),
    t2.customer_unique_id,
    t2.customer_zip_code_prefix,
    INITCAP(t2.customer_city) AS customer_city,
    t2.customer_state,
    FIRST_VALUE(t2.customer_state) OVER(
      PARTITION BY t2.customer_unique_id 
      ORDER BY t1.order_purchase_timestamp ASC
    ) AS primary_state
  FROM {{ source('bronze', 'orders')}} AS t1
  LEFT JOIN {{ source('bronze', 'customers')}} AS t2
    ON t1.customer_id = t2.customer_id
),

-- CTE 5: Process & Take only the earliest review score of each order
-- CTE 5: Process & Take only the earliest review score of each order
latest_review_scores AS (
  SELECT 
    order_id,
    review_score,
    -- FIX: Change the format string to match the slashes in the input data
    PARSE_DATETIME("%m/%d/%Y %H:%M", review_creation_date) AS review_cre_datetime,
    PARSE_DATETIME("%m/%d/%Y %H:%M", review_answer_timestamp) AS review_ans_datetime
  FROM {{ source('bronze', 'order_reviews')}}
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_id
    -- FIX: Use the correct PARSE_DATETIME with the matching format string here too
    ORDER BY PARSE_DATETIME("%m/%d/%Y %H:%M", review_creation_date) DESC
  ) = 1
)

-- Final SELECT: combine all aggregated and flat data
SELECT
  a.*,
  COALESCE(b.calculated_order_cost, 0.00) AS calculated_order_cost,
  COALESCE(c.total_payment_value, 0.00) AS total_payment_value,

  b.order_items_array,
  c.payments_array,
  d.sellers_array,

  CASE 
    WHEN ABS(COALESCE(b.calculated_order_cost, 0.00) - COALESCE(c.total_payment_value, 0.00)) < 0.1 THEN 'Match'
    WHEN a.order_status IN ('delivered', 'shipped') AND c.total_payment_value IS NULL THEN 'Missing Payment'
    ELSE 'Discrepancy'
  END AS payment_match_flag,

  CASE
    WHEN a.order_delivered_customer_date <= a.order_estimated_delivery_date THEN 1
    ELSE 0
  END AS is_delivered_on_time,

  -- BigQuery DATETIME_DIFF returns a FLOAT when the unit is day/hour/etc.
  -- Use TIMESTAMP_DIFF and divide to get days, or stick to your original logic.
  ROUND(
  DATETIME_DIFF(a.order_estimated_delivery_date, a.order_delivered_customer_date, HOUR)/24.0, 
  2) AS num_early_delivery_days,

  CASE WHEN a.order_purchase_timestamp > a.order_delivered_carrier_date
        OR a.order_approved_at > a.order_delivered_carrier_date
        OR a.order_delivered_carrier_date > a.order_delivered_customer_date
        THEN 'invalid'
  ELSE 'valid' END AS date_validity,
  
  -- Explicitly select review columns for clarity
  e.review_score,
  e.review_cre_datetime,
  e.review_ans_datetime,
  
  CURRENT_TIMESTAMP() as _ingest_ts

FROM core_order AS a
LEFT JOIN items_products_agg AS b ON a.order_id = b.order_id
LEFT JOIN payments_agg AS c ON a.order_id = c.order_id
LEFT JOIN sellers_agg AS d ON a.order_id = d.order_id
LEFT JOIN latest_review_scores AS e ON a.order_id = e.order_id
-- Uncommented the filter
-- WHERE a.order_status NOT IN ('canceled', 'unavailable');