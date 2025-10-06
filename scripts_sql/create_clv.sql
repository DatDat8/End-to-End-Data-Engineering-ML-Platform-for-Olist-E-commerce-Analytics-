CREATE OR REPLACE TABLE `gold.fct_customer_ltv` AS 
WITH customer_metric AS (
    SELECT
        customer_unique_id,
        primary_state,
        COUNT(DISTINCT order_id) AS total_orders,
        ROUND(SUM(calculated_order_cost), 2) AS total_spent, -- Monetary value (LTV input)
        MIN(DATE(order_purchase_timestamp)) AS first_purchase_date,
        MAX(DATE(order_purchase_timestamp)) AS last_purchase_date,
        
        -- Calculate Recency (Days since last purchase)
        DATE_DIFF(CAST('2018-09-03' AS DATE), MAX(DATE(order_purchase_timestamp)), DAY) AS recency_days

    FROM `silver.denormalized_orders`
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

        -- Customer's Historical AOV
        ROUND(SAFE_DIVIDE(total_spent, total_orders),2) AS historical_aov
    FROM customer_metric
)

-- 3. Combine Metrics and Calculate Projected LTV
SELECT
    cb.customer_unique_id,
    cb.primary_state,
    cb.days_since_first_purchase,
    cb.total_orders,
    cb.total_spent AS historical_ltv, -- Simplest LTV: what they've spent so far
    cb.historical_aov,
    -- Project a simplified 1-Year LTV (LTV-1Y): 
    -- Based on customer's AOV and estimated purchases in the next 6 months.
    -- We'll assume a conservative 6-month lifetime for the LTV factor.
    ROUND(cb.historical_aov * (
            -- Use the customer's *historical* monthly frequency to predict next 6 months
            SAFE_DIVIDE(cb.total_orders, (cb.days_since_first_purchase / 30.4)) * 6
    ), 2) AS predicted_ltv_1y_heuristic,

    -- Calculate total profit contribution (requires a gross margin input, placeholder 25%)
    cb.total_spent * 0.25 AS historical_profit_contribution

FROM customer_historical_aov AS cb;