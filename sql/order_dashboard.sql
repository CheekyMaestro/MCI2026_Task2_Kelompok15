
-- DASHBOARD 1: EXECUTIVE SUMMARY — Group 15

-- Q1: Total Orders | Chart: Number
SELECT COUNT(*) AS total_orders
FROM orders_analytics.orders;
 
-- Q2: Total Products Ordered | Chart: Number
SELECT COUNT(*) AS total_products_ordered
FROM orders_analytics.order_products;
 
-- Q3: Unique Users | Chart: Number
SELECT COUNT(DISTINCT user_id) AS unique_users
FROM orders_analytics.orders;
 
-- Q4: Average Basket Size | Chart: Number
SELECT ROUND(AVG(product_count), 2) AS avg_basket_size
FROM orders_analytics.orders;
 
-- Q5: Overall Reorder Rate (%) | Chart: Number
SELECT ROUND(SUM(reordered) * 100.0 / COUNT(*), 2) AS reorder_rate_pct
FROM orders_analytics.order_products;
 
 
 
-- Q6: Total Unique Products | Chart: Number
SELECT COUNT(DISTINCT product_name) AS unique_products
FROM orders_analytics.order_products;
 
-- Q7: Total Departments | Chart: Number
SELECT COUNT(DISTINCT department) AS total_departments
FROM orders_analytics.department_stats;
 
-- Q8: Total Aisles | Chart: Number
SELECT COUNT(*) AS total_aisles
FROM orders_analytics.aisle_stats;
 
 

 
-- Q9: Top 10 Departments by Volume

SELECT
    department,
    total_products_ordered
FROM orders_analytics.department_stats
ORDER BY total_products_ordered DESC
LIMIT 10;
 
 
-- Q10: Top 5 Department Proportion

SELECT
    category,
    SUM(total_products_ordered) AS total
FROM (
    SELECT
        department,
        total_products_ordered,
        CASE
            WHEN ROW_NUMBER() OVER (ORDER BY total_products_ordered DESC) <= 5
            THEN department
            ELSE 'Others'
        END AS category
    FROM orders_analytics.department_stats
) t
GROUP BY category
ORDER BY total DESC;
 
 

-- SECTION D: TOP PRODUCTS

SELECT
    product_name AS product,
    order_count  AS total_orders
FROM orders_analytics.top_products
ORDER BY order_count DESC
LIMIT 10;


-- DASHBOARD 2: PATTERN ANALYSIS — Group 15

-- Q1: Order by Time Period

SELECT
    CASE
        WHEN order_hour_of_day BETWEEN 0  AND 5  THEN 'Late Night (00-05)'
        WHEN order_hour_of_day BETWEEN 6  AND 11 THEN 'Morning (06-11)'
        WHEN order_hour_of_day BETWEEN 12 AND 17 THEN 'Afternoon (12-17)'
        WHEN order_hour_of_day BETWEEN 18 AND 23 THEN 'Evening (18-23)'
    END AS period,
    SUM(order_count) AS total_orders
FROM orders_analytics.hourly_distribution
GROUP BY period
ORDER BY total_orders DESC;
 
 
-- Q2: Weekday vs Weekend Proportion

SELECT
    CASE
        WHEN order_dow IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    SUM(order_count) AS total_orders
FROM orders_analytics.dow_distribution
GROUP BY day_type
ORDER BY total_orders DESC;
 
 
-- Q3: Orders by Day of Week

SELECT
    CASE order_dow
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END AS day,
    order_count AS total_orders,
    order_dow
FROM orders_analytics.dow_distribution
ORDER BY order_dow;
 
 
-- Q4: Orders by Hour of Day

SELECT
    order_hour_of_day AS hour,
    order_count       AS total_orders
FROM orders_analytics.hourly_distribution
ORDER BY order_hour_of_day;
 
 

--  "WHERE" — PRODUCT DISTRIBUTION

 
-- Q5: Top 15 Aisles

SELECT
    aisle                  AS aisle_name,
    department,
    total_products_ordered AS total_products
FROM orders_analytics.aisle_stats
ORDER BY total_products_ordered DESC
LIMIT 15;
 
 
-- Q6: Top 10 Aisles in Top Department

SELECT
    aisle                  AS aisle_name,
    total_products_ordered AS total_products
FROM orders_analytics.aisle_stats
WHERE department = 'produce'
ORDER BY total_products_ordered DESC
LIMIT 10;
 
 
-- Q7: Basket Size Distribution

SELECT
    product_count AS basket_size,
    COUNT(*)      AS order_count
FROM orders_analytics.orders
GROUP BY basket_size
ORDER BY basket_size;
 
 
-- Q8: Peak vs Off-Peak Hours

SELECT
    CASE
        WHEN order_hour_of_day IN (10, 11, 14, 15, 16, 17)
        THEN 'Peak Hours'
        ELSE 'Off-Peak Hours'
    END AS category,
    SUM(order_count) AS total_orders
FROM orders_analytics.hourly_distribution
GROUP BY category
ORDER BY total_orders DESC;

-- DASHBOARD 3: BUSINESS INSIGHTS — Group 15

 
-- Q1: Department Ranking by Reorder Rate

SELECT
    department,
    ROUND(reorder_rate, 2) AS reorder_rate_pct
FROM orders_analytics.department_stats
ORDER BY reorder_rate DESC;
 
 
-- Q2: Top 20 Products with Highest Reorder Rate

SELECT
    product_name     AS product,
    order_count      AS total_orders,
    ROUND(reorder_rate, 2) AS reorder_rate_pct
FROM orders_analytics.top_products
ORDER BY reorder_rate DESC
LIMIT 20;
 
 
-- Q3: Customer Loyalty Distribution

SELECT
    CASE
        WHEN reorder_rate > 60 THEN 'High Loyalty (>60%)'
        WHEN reorder_rate > 30 THEN 'Medium Loyalty (30-60%)'
        ELSE 'Low Loyalty (<30%)'
    END AS loyalty_level,
    COUNT(*) AS product_count
FROM orders_analytics.top_products
GROUP BY loyalty_level
ORDER BY product_count DESC;
 
-- Q4: Department: Total Products vs Reorder Rate

SELECT
    department,
    total_products_ordered AS total_products,
    ROUND(reorder_rate, 2) AS reorder_rate_pct
FROM orders_analytics.department_stats
ORDER BY total_products_ordered DESC;
 
 
-- Q5: Complete Product Leaderboard

SELECT
    ROW_NUMBER() OVER (ORDER BY order_count DESC) AS ranking,
    product_name AS product,
    order_count  AS total_orders,
    ROUND(reorder_rate, 2) AS reorder_rate_pct
FROM orders_analytics.top_products
ORDER BY order_count DESC;
 
 
-- Q6: Bottom 10 Products (Least Ordered)

SELECT
    product_name AS product,
    order_count  AS total_orders
FROM orders_analytics.top_products
ORDER BY order_count ASC
LIMIT 10;
 
 

 
-- Q7: Products: Order Count vs Reorder Rate (Scatter)

SELECT
    product_name     AS product,
    order_count      AS total_orders,
    ROUND(reorder_rate, 2) AS reorder_rate_pct
FROM orders_analytics.top_products
ORDER BY order_count DESC;
 
 
-- Q8: Reorder vs New Order

SELECT
    CASE reordered
        WHEN 1 THEN 'Reorder'
        WHEN 0 THEN 'New Order'
    END AS order_type,
    COUNT(*) AS count
FROM orders_analytics.order_products
GROUP BY reordered
ORDER BY count DESC;