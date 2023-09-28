-- Databricks notebook source
CREATE STREAMING LIVE TABLE sales_orders_cleaned(
  CONSTRAINT valid_order_number EXPECT (order_number IS NOT NULL) ON VIOLATION DROP ROW
  ,CONSTRAINT valid_city EXPECT (city IS NOT NULL AND city NOT LIKE '%ADDRESS%') ON VIOLATION DROP ROW 
)
PARTITIONED BY (order_date)
COMMENT "The cleaned sales orders with valid order_number(s) and partitioned by order_datetime."
TBLPROPERTIES ("quality" = "silver")
AS
SELECT 
  f.customer_id
  ,f.customer_name
  ,f.number_of_line_items
  ,TIMESTAMP(from_unixtime((cast(f.order_datetime as long)))) as order_datetime
  ,DATE(from_unixtime((cast(f.order_datetime as long)))) as order_date
  ,f.order_number
  ,f.ordered_products
  ,c.state
  ,c.city
  ,c.lon
  ,c.lat
  ,c.units_purchased
  ,c.loyalty_segment
FROM STREAM(LIVE.sales_orders_raw) f
  LEFT JOIN LIVE.customers c
      ON c.customer_id = f.customer_id
      AND c.customer_name = f.customer_name
