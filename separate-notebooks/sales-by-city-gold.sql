-- Databricks notebook source
CREATE LIVE TABLE sales_order_in_la
COMMENT "Sales orders in LA."
TBLPROPERTIES ("quality" = "gold")
AS
SELECT city, order_date, customer_id, customer_name, ordered_products_explode.curr,
SUM(ordered_products_explode.price) as sales,
SUM(ordered_products_explode.qty) as qantity,
COUNT(ordered_products_explode.id) as product_count
FROM (
SELECT city, DATE(order_datetime) as order_date, customer_id, customer_name,
EXPLODE(ordered_products) as ordered_products_explode
FROM LIVE.sales_orders_cleaned
WHERE city = 'Los Angeles'
  )
GROUP BY order_date, city, customer_id, customer_name, ordered_products_explode.curr;

-- COMMAND ----------

CREATE LIVE TABLE sales_order_in_chicago
COMMENT "Sales orders in Chicago."
TBLPROPERTIES ("quality" = "gold")
AS
SELECT city, order_date, customer_id, customer_name,
ordered_products_explode.curr,
SUM(ordered_products_explode.price) as sales,
SUM(ordered_products_explode.qty) as qantity,
COUNT(ordered_products_explode.id) as product_count
FROM (
  SELECT city, DATE(order_datetime) as order_date, customer_id, customer_name,
EXPLODE(ordered_products) as ordered_products_explode
  FROM LIVE.sales_orders_cleaned
  WHERE city = 'Chicago'
  )
GROUP BY order_date, city, customer_id, customer_name, ordered_products_explode.curr;
