{{
  config(
    materialized='table',
    schema='gold',
    alias='agg_cohort_retention_product',
    tags=['aggregate', 'retention', 'product']
  )
}}

WITH
-- 1. Determine the Customer's Fixed Cohort Category and Month
-- This CTE determines the single, defining category for the customer's acquisition cohort.
customer_fixed_cohort AS (
    SELECT
        t1.customer_unique_id,
        
        -- The fixed acquisition month for the customer
        DATE_TRUNC(
            MIN(t1.order_purchase_timestamp) OVER (PARTITION BY t1.customer_unique_id), MONTH
        ) AS cohort_month,

        -- The fixed category associated with the VERY FIRST purchase line item
        FIRST_VALUE(item.product_category) OVER (
            PARTITION BY t1.customer_unique_id
            ORDER BY t1.order_purchase_timestamp ASC, item.order_item_id ASC
        ) AS fixed_cohort_category_name
        
    -- Use dbt ref() macro for lineage
    FROM {{ ref('denormalized_orders') }} AS t1,
    UNNEST(t1.order_items_array) AS item
    WHERE t1.order_status = 'delivered'
),

-- 2. Map all Subsequent Orders to their Fixed Cohort
customer_purchase_months AS (
    SELECT DISTINCT
        t1.customer_unique_id,
        t2.fixed_cohort_category_name,
        t2.cohort_month,
        
        -- The month of the current (or subsequent) purchase
        DATE_TRUNC(t1.order_purchase_timestamp, MONTH) AS purchase_month,
        
        -- Calculate Retention Period
        DATE_DIFF(
            DATE_TRUNC(t1.order_purchase_timestamp, MONTH), 
            t2.cohort_month, 
            MONTH
        ) AS retention_period
        
    FROM {{ ref('denormalized_orders') }} AS t1
    INNER JOIN customer_fixed_cohort AS t2
        ON t1.customer_unique_id = t2.customer_unique_id
    WHERE t1.order_status = 'delivered'
),

-- 3. Calculate Final Cohort Counts (Grouped by Cohort Definition and Period)
final_cohort_counts AS (
    SELECT
        fixed_cohort_category_name,
        cohort_month,
        retention_period,
        COUNT(DISTINCT customer_unique_id) AS retained_customers
    FROM customer_purchase_months
    GROUP BY 1, 2, 3
)

-- Final SELECT: Calculate the Retention Rate
SELECT
    t1.fixed_cohort_category_name AS cohort_category_name,
    t1.cohort_month,
    FORMAT_DATE('%b %Y', t1.cohort_month) AS month_year,
    t1.retention_period,
    t1.retained_customers,
    t2.retained_customers AS initial_cohort_size,
    SAFE_DIVIDE(t1.retained_customers, t2.retained_customers) AS retention_rate,
    CURRENT_TIMESTAMP() as _ingest_ts

FROM final_cohort_counts AS t1
INNER JOIN final_cohort_counts AS t2
    ON t1.fixed_cohort_category_name = t2.fixed_cohort_category_name
    AND t1.cohort_month = t2.cohort_month
    AND t2.retention_period = 0
WHERE t1.retention_period BETWEEN 0 AND 6
ORDER BY 1, 2, 4