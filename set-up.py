# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog lakehouse_dev;
# MAGIC use schema retail;
# MAGIC show tables;

# COMMAND ----------

# MAGIC %md 
# MAGIC Let's say we work for a retail organization, and an OLTP system has dumped a bunch of files to our datalake for us to build reports or machine learning models on.  
# MAGIC
# MAGIC Some of the data might arrive in batch, some might be updated continously.  
# MAGIC
# MAGIC Our boss has asked us to review customer sales in LA and Chichago.  

# COMMAND ----------

# MAGIC %sql
# MAGIC list "/databricks-datasets/retail-org/"

# COMMAND ----------

# MAGIC %sql
# MAGIC list "/databricks-datasets/retail-org/customers/"

# COMMAND ----------

# MAGIC %sh
# MAGIC cat "/databricks-datasets/retail-org/customers/customers.csv"

# COMMAND ----------

# MAGIC %sql
# MAGIC list "/databricks-datasets/retail-org/sales_orders/"

# COMMAND ----------

# MAGIC %sh 
# MAGIC cat /databricks-datasets/retail-org/sales_orders/part-00000-tid-1771549084454148016-e2275afd-a5bb-40ed-b044-1774c0fdab2b-105592-1-c000.json

# COMMAND ----------

# MAGIC %md 
# MAGIC Looks like this is a nested JSON object so to print it nicely we'll need a little extra code.  Let's use the new Databricks Assistant to get the right python code to display the JSON in a nice format.  Let's try:
# MAGIC
# MAGIC > "I have a malformed nested JSON file.  How can I use python to print it nicely?"

# COMMAND ----------

sales_order_path = "/databricks-datasets/retail-org/sales_orders/part-00000-tid-1771549084454148016-e2275afd-a5bb-40ed-b044-1774c0fdab2b-105592-1-c000.json"

# COMMAND ----------

import json

with open(sales_order_path) as f:
    j = f.read()
    j = json.loads(j)
    print(json.dumps(j, indent=4))

# COMMAND ----------

with open(sales_order_path) as file:
    j = file.read()
    json_objs = j.strip().split("\n")
    for obj in json_objs:
        try:
            parsed_obj = json.loads(obj)
            print(json.dumps(parsed_obj, indent=4))
        except json.JSONDecodeError as err:
            print(f"Failed to parse json object: {obj}")
            print(f"Error: {err}")

# COMMAND ----------

import pyspark.sql

df = spark.read.json(sales_order_path)

# COMMAND ----------

display(df)
