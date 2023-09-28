-- Databricks notebook source
CREATE STREAMING LIVE TABLE customers
COMMENT "The customers buying finished products, ingested from /databricks-datasets."
TBLPROPERTIES ("quality" = "mapping")
AS SELECT * FROM cloud_files("/databricks-datasets/retail-org/customers/", "csv");
