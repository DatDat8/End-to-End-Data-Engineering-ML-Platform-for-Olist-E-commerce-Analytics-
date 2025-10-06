CREATE OR REPLACE TABLE `gold.agg_cohort_retention_all` AS
WITH
-- 1. Determine Cohort Month and Retention Period in one pass over the Silver table
retention_periods AS (
    SELECT
        -- Use the pre-joined customer_unique_id from the Silver table
        t1.customer_unique_id,
        t1.order_purchase_timestamp,
        DATE_TRUNC(t1.order_purchase_timestamp, MONTH) AS purchase_month,
        
        -- Use a Window Function to find the FIRST purchase month for each unique customer
        DATE_TRUNC(
            MIN(t1.order_purchase_timestamp) OVER (
                PARTITION BY t1.customer_unique_id
            ), MONTH
        ) AS cohort_month
        
    FROM `silver.denormalized_orders` AS t1
    -- Filter out non-delivered orders as a customer retention event requires a completed purchase
    WHERE t1.order_status = 'delivered'
),

-- 2. Calculate the final cohort counts and the month difference
final_cohort_counts AS (
    SELECT
        cohort_month,
        -- Calculate the number of months elapsed from the cohort month
        DATE_DIFF(purchase_month, cohort_month, MONTH) AS retention_period,
        
        -- Count the unique customers retained in that specific period
        COUNT(DISTINCT customer_unique_id) AS retained_customers
        
    FROM retention_periods
    -- Group by the cohort month and the retention period
    GROUP BY 1, 2
)

-- Final SELECT: Calculate the Retention Rate (%) and filter for periods 0-6
SELECT
    t1.cohort_month,
    FORMAT_DATE('%b %Y', t1.cohort_month) AS month_year,
    t1.retention_period,
    t1.retained_customers,
    
    -- Join back to Period 0 to get the initial size of the cohort
    t2.retained_customers AS initial_cohort_size,
    
    -- Calculate Retention Rate
    SAFE_DIVIDE(t1.retained_customers, t2.retained_customers) AS retention_rate

FROM final_cohort_counts AS t1
INNER JOIN final_cohort_counts AS t2
    ON t1.cohort_month = t2.cohort_month
    AND t2.retention_period = 0 -- Join key for the initial size
WHERE t1.retention_period BETWEEN 0 AND 6 -- Only show 7 months (0 to 6)
ORDER BY 1, 3;