--- Check for NULLs & invalid Negative cost 
--- Expect no results
SELECT *
FROM olist-ecommerce-dw.bronze.orders
WHERE order_id IS NULL OR customer_id IS NULL OR order_status IS NULL;

--- Check for duplicates on primary keys
--- Expext no results
--- The customer_id seems redundant in the customers table but it shows that a customer can have different order locations on different orders
SELECT
  order_id,
  COUNT(*) AS dup_ord_id
FROM olist-ecommerce-dw.bronze.orders
GROUP BY order_id
HAVING COUNT(*)>1;

SELECT COUNT(DISTINCT customer_id)/(SELECT COUNT(*) FROM bronze.customers)
FROM `bronze.customers`; 

SELECT COUNT(DISTINCT customer_id)/(SELECT COUNT(*) FROM bronze.orders) 
FROM bronze.orders;

SELECT customer_id, COUNT(*)
FROM bronze.orders
GROUP BY customer_id
HAVING COUNT(*)>1;

SELECT product_id, COUNT(*)
FROM `bronze.products`
GROUP BY product_id
HAVING COUNT(*)>1;

SELECT order_id, COUNT(*)
FROM `bronze.order_payments`
GROUP BY order_id
HAVING COUNT(*)>1;

SELECT customer_unique_id, COUNT(*)
FROM bronze.customers
GROUP BY customer_unique_id
HAVING COUNT(*)>1;

SELECT COUNT(DISTINCT order_id)
FROM bronze.orders;

SELECT COUNT(DISTINCT customer_id)
FROM bronze.orders;

SELECT *
FROM bronze.customers AS c1
LEFT JOIN bronze.customers AS c2
ON c1.customer_unique_id = c2.customer_unique_id
WHERE c1.customer_zip_code_prefix !=c2.customer_zip_code_prefix
OR c1.customer_city !=c2.customer_city;

SELECT product_id, COUNT(*)
FROM bronze.products
GROUP BY product_id
HAVING COUNT(*) >1;


-- It appears that some orders has multiple reviews on different time 
SELECT t1.* EXCEPT(review_creation_date), PARSE_DATETIME("%m/%d/%Y %H:%M",t1.review_creation_date) AS parsed_cre_date
FROM bronze.order_reviews AS t1
RIGHT JOIN (
SELECT order_id, COUNT(*)
FROM bronze.order_reviews
GROUP BY order_id
HAVING COUNT(*) >1) AS t2 ON t1.order_id = t2.order_id
ORDER BY t1.order_id;

-- For analysis, only the earliest reviews are selected for the same order
SELECT t1.*
FROM (  SELECT 
    order_id,
    review_score,
    PARSE_DATETIME("%m/%d/%Y %H:%M",review_creation_date) AS review_cre_date,
    PARSE_DATETIME("%m/%d/%Y %H:%M",review_answer_timestamp) AS review_ans_datetime
  FROM `bronze.order_reviews`
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_id
    ORDER BY PARSE_DATETIME("%m/%d/%Y %H:%M", review_creation_date) DESC
  ) = 1
  ORDER BY order_id
) AS t1
RIGHT JOIN (
SELECT order_id, COUNT(*)
FROM bronze.order_reviews
GROUP BY order_id
HAVING COUNT(*) >1) AS t2 ON t1.order_id = t2.order_id
ORDER BY t1.order_id;




SELECT *
FROM `bronze.products`
WHERE product_category_name NOT IN (SELECT DISTINCT product_category_name FROM bronze.product_category_name_translation);

SELECT *
FROM `bronze.orders`
WHERE order_id NOT IN (SELECT DISTINCT order_id FROM bronze.order_reviews);

SELECT *
FROM `bronze.order_reviews`
WHERE order_id NOT IN (SELECT DISTINCT order_id FROM bronze.orders);

SELECT *
FROM `bronze.order_items`
WHERE product_id NOT IN (SELECT DISTINCT product_id FROM bronze.products);

--- Check for invalid order date & delivery
--- Expect 0 as the rate over the total number of records
--- detected 1.4% of rows with invalid date time
--- correct by giving them flags
SELECT * 
FROM olist-ecommerce-dw.bronze.orders
WHERE 
order_purchase_timestamp > order_approved_at
OR order_purchase_timestamp > order_delivered_carrier_date
OR order_purchase_timestamp > order_delivered_customer_date
OR order_approved_at > order_delivered_carrier_date
OR order_delivered_carrier_date > order_delivered_customer_date;



--- Check for connectivity
--- Expect no result
--- Detected 1 order record without payment
--- there're orders without price details, which are all canceled
SELECT *
FROM olist-ecommerce-dw.bronze.orders
WHERE order_id NOT IN (SELECT DISTINCT order_id FROM olist-ecommerce-dw.bronze.order_payments);

SELECT *
FROM olist-ecommerce-dw.bronze.orders
WHERE order_id NOT IN (SELECT DISTINCT order_id FROM olist-ecommerce-dw.bronze.order_items);  

--- Check for data consistency between order value and payment 
--- There appears 259 orders where payment doesn't match the total order value 
--- This shows data inconsistency for payment match -> flag required
--- There are 467 orders with canceled or unavailable status, which should not contribute to analytic results and should be removed for silver layer
WITH order_totals AS (
  SELECT
    a.order_id,
    a.order_status,
    ROUND(SUM(b.price + b.freight_value), 2) AS calculated_total_price
  FROM olist-ecommerce-dw.bronze.orders AS a
  INNER JOIN olist-ecommerce-dw.bronze.order_items AS b
    ON a.order_id = b.order_id
  GROUP BY 1, 2
),
order_payments_agg AS (
  SELECT
    order_id,
    ROUND(SUM(payment_value), 2) AS total_payment_value
  FROM olist-ecommerce-dw.bronze.order_payments
  GROUP BY 1
)
SELECT
  t.order_id,
  t.order_status,
  t.calculated_total_price,
  p.total_payment_value
FROM order_totals AS t
INNER JOIN order_payments_agg AS p
  ON t.order_id = p.order_id
WHERE ABS(t.calculated_total_price - p.total_payment_value) > 0.1; 
-- t.order_status NOT IN ('delivered', 'shipped', 'invoiced', 'processing', 'approved')









