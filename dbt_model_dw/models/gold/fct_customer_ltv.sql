{{
  config(
    materialized='table',
    schema='gold',
    alias='fct_customer_ltv',
    tags=['fact', 'customer']
  )
}}

WITH customer_metric AS (
    SELECT
        customer_unique_id,
        primary_state,
        COUNT(DISTINCT order_id) AS total_orders,
        ROUND(SUM(calculated_order_cost), 2) AS total_spent, -- Historical Monetary Value
        MIN(DATE(order_purchase_timestamp)) AS first_purchase_date,
        MAX(DATE(order_purchase_timestamp)) AS last_purchase_date,
        
        -- Recency: Days since last purchase, relative to model run time
        DATE_DIFF(
            CURRENT_DATE(), -- FIX: Use CURRENT_DATE() instead of hardcoded date
            MAX(DATE(order_purchase_timestamp)), 
            DAY
        ) AS recency_days

    -- FIX: Use dbt ref() macro for lineage
    FROM {{ ref('denormalized_orders') }} 
    WHERE order_status = 'delivered'
    GROUP BY 1, 2
),
customer_historical_aov AS (
    SELECT
        customer_unique_id,
        primary_state,
        total_orders,
        total_spent,
        -- Calculate time difference. Use NULLIF to prevent division by zero later.
        DATE_DIFF(last_purchase_date, first_purchase_date, DAY) AS days_since_first_purchase,

        -- Customer's Historical Average Order Value (AOV)
        ROUND(SAFE_DIVIDE(total_spent, total_orders), 2) AS historical_aov
    FROM customer_metric
)

-- 3. Combine Metrics and Calculate Projected LTV
SELECT
    cb.customer_unique_id,
    cb.primary_state,
    cb.days_since_first_purchase,
    cb.total_orders,
    cb.total_spent AS historical_ltv, -- What they've spent so far
    cb.historical_aov,

    -- Project a simplified 6-Month LTV Heuristic: 
    -- AOV * (Monthly Purchase Frequency * 6)
    ROUND(cb.historical_aov * (
                SAFE_DIVIDE(
                    cb.total_orders, 
                    -- FIX: Ensure denominator is not zero. Use COALESCE(NULLIF(..., 0), 1) pattern
                    -- If days_since_first_purchase is 0, set to a small positive number (e.g., 30.4 days)
                    NULLIF(cb.days_since_first_purchase, 0) / 30.4
                ) * 6
    ), 2) AS predicted_ltv_6m_heuristic, -- Renamed to 6M for clarity of the factor '6'

    -- Calculate historical profit contribution (using a placeholder 25% gross margin)
    ROUND(cb.total_spent * 0.25, 2) AS historical_profit_contribution

FROM customer_historical_aov AS cb