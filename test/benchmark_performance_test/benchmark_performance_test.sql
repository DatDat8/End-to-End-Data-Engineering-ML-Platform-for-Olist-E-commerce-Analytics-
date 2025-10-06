/*
==================================================================================================
Benmark for measuring the performance of creating gold tables between the traditional tables JOINS
vs. query from the ARRAY structure denormalized in silver layer  
==================================================================================================
*/

-- Table 1: fct_customer_ltv (Traditional Bronze Joins)
-- Re-runs joins across multiple Bronze tables
-- CREATE OR REPLACE TABLE `benchmark_join.bm_fct_customer_ltv` AS
WITH core_order AS (
    -- Recreate the core order and customer flat data for LTV calculation
    SELECT
        t1.order_id,
        t1.order_status,
        t1.order_purchase_timestamp,
        t2.customer_unique_id,
        
        -- Calculate the monetary value (cost) by aggregating items
        (SELECT ROUND(SUM(price + freight_value), 2) FROM `bronze.order_items` WHERE order_id = t1.order_id) AS calculated_order_cost,
        
        -- Calculate the primary state (required for the final LTV model)
        FIRST_VALUE(t2.customer_state) OVER(
             PARTITION BY t2.customer_unique_id 
             ORDER BY t1.order_purchase_timestamp ASC
        ) AS primary_state
        
    FROM `bronze.orders` AS t1
    INNER JOIN `bronze.customers` AS t2
        ON t1.customer_id = t2.customer_id
),
customer_metric AS (
    SELECT
        customer_unique_id,
        primary_state,
        COUNT(DISTINCT order_id) AS total_orders,
        ROUND(SUM(calculated_order_cost), 2) AS total_spent,
        MIN(DATE(order_purchase_timestamp)) AS first_purchase_date,
        MAX(DATE(order_purchase_timestamp)) AS last_purchase_date,
        
        -- Recency
        DATE_DIFF(CURRENT_DATE(), MAX(DATE(order_purchase_timestamp)), DAY) AS recency_days

    FROM core_order
    WHERE order_status = 'delivered'
    GROUP BY 1, 2
),
customer_historical_aov AS (
    SELECT
        customer_unique_id,
        primary_state,
        total_orders,
        total_spent,
        DATE_DIFF(last_purchase_date, first_purchase_date, DAY) AS days_since_first_purchase,
        ROUND(SAFE_DIVIDE(total_spent, total_orders),2) AS historical_aov
    FROM customer_metric
)

-- Final SELECT: Combine Metrics and Calculate Projected LTV
SELECT
    cb.customer_unique_id,
    cb.primary_state,
    cb.days_since_first_purchase,
    cb.total_orders,
    cb.total_spent AS historical_ltv,
    cb.historical_aov,
    
    ROUND(cb.historical_aov * (
                SAFE_DIVIDE(
                    cb.total_orders, 
                    NULLIF(cb.days_since_first_purchase, 0) / 30.4
                ) * 6
    ), 2) AS predicted_ltv_6m_heuristic,

    ROUND(cb.total_spent * 0.25, 2) AS historical_profit_contribution

FROM customer_historical_aov AS cb;

-- ==================================================================================================

-- Gold Table 2: agg_payment_behavior (Traditional Bronze Joins)
-- Re-runs joins across orders, order_items, order_payments, and order_reviews
-- CREATE OR REPLACE TABLE `dbt_olist_dw_gold.agg_payment_behavior_traditional_join` AS
WITH order_base_metrics AS (
    -- Calculate aggregated metrics at the order level first
    SELECT
        t1.order_id,
        t1.order_status,
        -- Calculate total cost from items (used for payment match)
        ROUND(SUM(t2.price + t2.freight_value), 2) AS calculated_order_cost,
        -- Calculate total payment value (used for payment match)
        ROUND(SUM(t3.payment_value), 2) AS total_payment_value
    FROM `bronze.orders` AS t1
    LEFT JOIN `bronze.order_items` AS t2 
        ON t1.order_id = t2.order_id
    LEFT JOIN `bronze.order_payments` AS t3 
        ON t1.order_id = t3.order_id
    GROUP BY 1, 2
),
order_reviews_agg AS (
    -- Get the single review score per order (similar to your CTE 5)
    SELECT 
        order_id,
        review_score
    FROM `bronze.order_reviews`
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_id
        ORDER BY PARSE_TIMESTAMP("%m/%d/%Y %H:%M", review_creation_date) DESC -- Assuming this is the correct format for Bronze
    ) = 1
)

-- Final Aggregation: Join items, payments, base metrics, and reviews
SELECT
    p.payment_type,
    bm.order_status,
    -- Join to product category translation to get the category name
    INITCAP(REPLACE(
      COALESCE(trans.product_category_name_english, pr.product_category_name)
    ,'_', ' ')) AS product_category,

    -- BEHAVIOR METRICS
    COUNT(DISTINCT bm.order_id) AS total_dist_orders,
    COUNT(*) AS total_order_items, 
    ROUND(SUM(bm.calculated_order_cost), 2) AS total_gmv,
    ROUND(SUM(bm.calculated_order_cost - bm.total_payment_value), 2) AS payment_missing_value,
    ROUND(SUM(p.payment_installments), 2) AS total_installments,
    
    -- DERIVED MATCH FLAG AND RISK METRICS
    SUM(CASE 
        WHEN ABS(COALESCE(bm.calculated_order_cost,0.00) - COALESCE(bm.total_payment_value,0.00)) < 0.1 THEN 0 -- Match
        WHEN bm.order_status IN ('delivered', 'shipped') AND bm.total_payment_value IS NULL THEN 1 -- Missing Payment
        ELSE 1 -- Discrepancy
    END) AS mismatch_transactions_count,
    
    ROUND(SAFE_DIVIDE(
        SUM(CASE 
            WHEN ABS(COALESCE(bm.calculated_order_cost,0.00) - COALESCE(bm.total_payment_value,0.00)) < 0.1 THEN 0 
            ELSE 1 
        END),
        COUNT(DISTINCT bm.order_id)
    ), 4) AS payment_mismatch_rate,
    
    ROUND(SUM(r.review_score), 2) AS total_review_score

FROM order_base_metrics AS bm
INNER JOIN `bronze.order_items` AS oi
    ON bm.order_id = oi.order_id
INNER JOIN `bronze.order_payments` AS p
    ON bm.order_id = p.order_id
LEFT JOIN `bronze.products` AS pr
    ON oi.product_id = pr.product_id
LEFT JOIN `bronze.product_category_name_translation` AS trans
    ON pr.product_category_name = trans.product_category_name
LEFT JOIN order_reviews_agg AS r
    ON bm.order_id = r.order_id

GROUP BY 1, 2, 3
ORDER BY 1, 2, mismatch_transactions_count DESC;



-- ==================================================================================================


-- Gold Table 3: fct_seller_performance (Traditional Bronze Joins)
-- Re-runs joins across orders, order_items, sellers, and order_reviews
-- CREATE OR REPLACE TABLE `dbt_olist_dw_gold.fct_seller_performance_traditional_join` AS
WITH order_reviews_agg AS (
    -- Get the single review score per order (similar to your CTE 5)
    SELECT 
        order_id,
        review_score
    FROM `bronze.order_reviews`
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_id
        ORDER BY PARSE_TIMESTAMP("%m/%d/%Y %H:%M", review_creation_date) DESC
    ) = 1
),
translated_items AS (
    -- Prepare item data with the translated product category
    SELECT
        t1.order_id,
        t1.seller_id,
        t1.price,
        t2.product_category_name,
        INITCAP(REPLACE(
          COALESCE(t3.product_category_name_english, t2.product_category_name)
          ,'_', ' ')) AS product_category
    FROM `bronze.order_items` AS t1
    LEFT JOIN `bronze.products` AS t2 
        ON t1.product_id = t2.product_id
    LEFT JOIN `bronze.product_category_name_translation` AS t3
        ON t2.product_category_name = t3.product_category_name
)

-- Final Aggregation: Join orders, sellers, items, and reviews
SELECT
    s.seller_id,
    s.seller_state,
    ti.product_category,
    
    COUNT(t.order_id) AS total_order_items_sold, 
    ROUND(SUM(ti.price),2) AS total_revenue_contribution,
    
    -- On-Time Delivery Rate
    -- Need to join order data to get delivery dates
    ROUND(
        SUM(CASE
            WHEN t.order_delivered_customer_date <= t.order_estimated_delivery_date
            THEN 1 ELSE 0
        END) / COUNT(DISTINCT t.order_id), 4) AS on_time_delivery_rate,

    -- Average Rating
    ROUND(AVG(r.review_score), 2) AS average_review_score
    
FROM `bronze.orders` AS t
INNER JOIN translated_items AS ti 
    ON t.order_id = ti.order_id
INNER JOIN `bronze.sellers` AS s
    ON ti.seller_id = s.seller_id
LEFT JOIN order_reviews_agg AS r
    ON t.order_id = r.order_id
    
WHERE t.order_status = 'delivered'
GROUP BY 1,2,3
ORDER BY 1,3,2;


-- ==================================================================================================

-- Gold Table 4: agg_sales_kpi (Traditional Bronze Joins)
-- Re-runs joins across orders, customers, order_items, and calculates all flags/metrics
-- Query 2: agg_sales_kpi (Traditional Bronze Joins) - FIXED CORRELATED SUBQUERY ERROR
-- CTE 1: Aggregate Costs (calculated_order_cost)
WITH order_costs_agg AS (
    SELECT
        order_id,
        ROUND(SUM(price + freight_value), 2) AS calculated_order_cost
    FROM `bronze.order_items`
    GROUP BY 1
),
-- CTE 2: Aggregate Payments (total_payment_value)
order_payments_agg AS (
    SELECT
        order_id,
        ROUND(SUM(payment_value), 2) AS total_payment_value
    FROM `bronze.order_payments`
    GROUP BY 1
),
-- CTE 3: Get the single Review Score (De-correlated version of CTE 5 from Silver)
order_reviews_agg AS (
    SELECT 
        order_id,
        review_score
    FROM `bronze.order_reviews`
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_id
        ORDER BY PARSE_TIMESTAMP("%m/%d/%Y %H:%M", review_creation_date) DESC
    ) = 1
),
-- CTE 4: Core Order Metrics (Flat Order/Customer Data + Join Aggregates)
order_metrics AS (
    SELECT
        t1.order_id,
        t1.order_status,
        t1.order_purchase_timestamp,
        t1.order_delivered_customer_date,
        t1.order_estimated_delivery_date,
        
        t3.customer_unique_id,
        INITCAP(t3.customer_city) AS customer_city,
        t3.customer_state,
        
        -- NOW JOINED:
        coa.calculated_order_cost,
        poa.total_payment_value,
        rva.review_score

    FROM `bronze.orders` AS t1
    INNER JOIN `bronze.customers` AS t3 
        ON t1.customer_id = t3.customer_id
    LEFT JOIN order_costs_agg AS coa 
        ON t1.order_id = coa.order_id
    LEFT JOIN order_payments_agg AS poa 
        ON t1.order_id = poa.order_id
    LEFT JOIN order_reviews_agg AS rva
        ON t1.order_id = rva.order_id
),
-- CTE 5: Calculate Derived Flags/KPIs (Logic remains the same)
order_kpis AS (
    SELECT
        *,
        -- Payment Match Flag (Recalculated)
        CASE 
            WHEN ABS(COALESCE(calculated_order_cost, 0.00) - COALESCE(total_payment_value, 0.00)) < 0.1 THEN 'Match'
            WHEN order_status IN ('delivered', 'shipped') AND total_payment_value IS NULL THEN 'Missing Payment'
            ELSE 'Discrepancy'
        END AS payment_match_flag,

        -- Delivery KPIs (Recalculated)
        CASE
            WHEN order_delivered_customer_date <= order_estimated_delivery_date THEN 1
            ELSE 0
        END AS is_delivered_on_time,
        
        ROUND(
            DATETIME_DIFF(order_estimated_delivery_date, order_delivered_customer_date, HOUR)/24.0, 
        2) AS num_early_delivery_days
    FROM order_metrics
),
-- CTE 6: Get Item Category and join back to Orders (Logic remains the same)
order_items_categorized AS (
    SELECT
        t1.order_id,
        INITCAP(REPLACE(
            COALESCE(t3.product_category_name_english, t2.product_category_name)
        , '_', ' ')) AS product_category
    FROM `bronze.order_items` AS t1
    LEFT JOIN `bronze.products` AS t2 
        ON t1.product_id = t2.product_id
    LEFT JOIN `bronze.product_category_name_translation` AS t3
        ON t2.product_category_name = t3.product_category_name
)

-- Final SELECT: Join KPIs and Item Categories, then Group
SELECT
    item.product_category,
    kpi.order_status,
    kpi.payment_match_flag,
    kpi.customer_state,
    kpi.customer_city,
    
    ROUND(SUM(kpi.calculated_order_cost), 2) AS total_sales,
    COUNT(DISTINCT kpi.customer_unique_id) AS total_customers,
    COUNT(DISTINCT kpi.order_id) AS total_orders,
    
    ROUND(SUM(kpi.num_early_delivery_days), 2) AS total_early_delivery_days,
    SUM(kpi.is_delivered_on_time) AS total_on_time_delivery,
    
    SUM(kpi.review_score) AS total_review_score

FROM order_kpis AS kpi
INNER JOIN order_items_categorized AS item
    ON kpi.order_id = item.order_id
GROUP BY 1, 2, 3, 4, 5;



-- ==================================================================================================

-- Gold Table 5: agg_cohort_retention_all (Traditional Bronze Joins)
-- Re-runs joins across orders and customers
-- CREATE OR REPLACE TABLE `dbt_olist_dw_gold.agg_cohort_retention_all_traditional_join` AS
-- CTE 1: Get the required fields from Bronze and determine cohort month
WITH retention_periods AS (
    SELECT
        t2.customer_unique_id,
        t1.order_purchase_timestamp,
        DATE_TRUNC(t1.order_purchase_timestamp, MONTH) AS purchase_month,
        
        -- Use a Window Function to find the FIRST purchase month for each unique customer
        DATE_TRUNC(
            MIN(t1.order_purchase_timestamp) OVER (
                PARTITION BY t2.customer_unique_id
            ), MONTH
        ) AS cohort_month
        
    FROM `bronze.orders` AS t1
    INNER JOIN `bronze.customers` AS t2
        ON t1.customer_id = t2.customer_id
    WHERE t1.order_status = 'delivered'
),

-- CTE 2: Calculate the final cohort counts and the month difference
final_cohort_counts AS (
    SELECT
        cohort_month,
        DATE_DIFF(purchase_month, cohort_month, MONTH) AS retention_period,
        COUNT(DISTINCT customer_unique_id) AS retained_customers
    FROM retention_periods
    GROUP BY 1, 2
)

-- Final SELECT: Calculate the Retention Rate (%) and filter for periods 0-6
SELECT
    t1.cohort_month,
    FORMAT_DATE('%b %Y', t1.cohort_month) AS month_year,
    t1.retention_period,
    t1.retained_customers,
    t2.retained_customers AS initial_cohort_size,
    SAFE_DIVIDE(t1.retained_customers, t2.retained_customers) AS retention_rate

FROM final_cohort_counts AS t1
INNER JOIN final_cohort_counts AS t2
    ON t1.cohort_month = t2.cohort_month
    AND t2.retention_period = 0 
WHERE t1.retention_period BETWEEN 0 AND 6 
ORDER BY 1, 3;


-- ==================================================================================================
-- Gold Table 5: agg_cohort_retention_product (Traditional Bronze Joins)
-- Re-runs joins across all necessary Bronze tables
-- CREATE OR REPLACE TABLE `dbt_olist_dw_gold.agg_cohort_retention_product_traditional_join` AS
WITH
-- CTE 1: Combine Orders, Customers, and Items (Simplified for direct use)
customer_order_item_details AS (
    SELECT
        t2.customer_unique_id,
        t1.order_status,
        t1.order_purchase_timestamp,
        t4.order_item_id,
        
        INITCAP(REPLACE(
            COALESCE(t6.product_category_name_english, t5.product_category_name)
        , '_', ' ')) AS product_category
        
    FROM `bronze.orders` AS t1
    INNER JOIN `bronze.customers` AS t2
        ON t1.customer_id = t2.customer_id
    INNER JOIN `bronze.order_items` AS t4
        ON t1.order_id = t4.order_id
    LEFT JOIN `bronze.products` AS t5 
        ON t4.product_id = t5.product_id
    LEFT JOIN `bronze.product_category_name_translation` AS t6
        ON t5.product_category_name = t6.product_category_name
    WHERE t1.order_status = 'delivered'
),

-- CTE 2: Determine the Customer's Fixed Cohort Category and Month
customer_fixed_cohort AS (
    SELECT
        customer_unique_id,
        
        -- The fixed acquisition month for the customer
        DATE_TRUNC(
            MIN(order_purchase_timestamp) OVER (PARTITION BY customer_unique_id), MONTH
        ) AS cohort_month,

        -- The fixed category associated with the VERY FIRST purchase line item
        FIRST_VALUE(product_category) OVER (
            PARTITION BY customer_unique_id
            ORDER BY order_purchase_timestamp ASC, order_item_id ASC
        ) AS fixed_cohort_category_name
    FROM customer_order_item_details
),

-- CTE 3: Map all Subsequent Orders to their Fixed Cohort
customer_purchase_months AS (
    SELECT DISTINCT
        t1.customer_unique_id,
        t2.fixed_cohort_category_name,
        t2.cohort_month,
        
        -- Use the purchase timestamp from the full order list (t1)
        DATE_TRUNC(t1.order_purchase_timestamp, MONTH) AS purchase_month,
        
        DATE_DIFF(
            DATE_TRUNC(t1.order_purchase_timestamp, MONTH), 
            t2.cohort_month, 
            MONTH
        ) AS retention_period
        
    -- NOTE: We must join back to ALL orders/customers to capture subsequent purchases
    FROM `bronze.orders` AS t1
    INNER JOIN `bronze.customers` AS t_cust 
        ON t1.customer_id = t_cust.customer_id
    INNER JOIN customer_fixed_cohort AS t2
        ON t_cust.customer_unique_id = t2.customer_unique_id -- Joins by unique customer ID
    WHERE t1.order_status = 'delivered'
    -- The issue was likely selecting `customer_unique_id` from a table that didn't expose it after joins
    -- In this fixed version, we select `t_cust.customer_unique_id` (implicitly via DISTINCT)
),

-- CTE 4: Calculate Final Cohort Counts
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
    SAFE_DIVIDE(t1.retained_customers, t2.retained_customers) AS retention_rate

FROM final_cohort_counts AS t1
INNER JOIN final_cohort_counts AS t2
    ON t1.fixed_cohort_category_name = t2.fixed_cohort_category_name
    AND t1.cohort_month = t2.cohort_month
    AND t2.retention_period = 0
WHERE t1.retention_period BETWEEN 0 AND 6
ORDER BY 1, 2, 4;