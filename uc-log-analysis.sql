-- Databricks notebook source
use catalog lakehouse_dev;
use schema retail;
show tables;

-- COMMAND ----------

SELECT * FROM event_log(TABLE(sales_orders_cleaned))

-- COMMAND ----------

CREATE OR REPLACE VIEW event_log_raw AS SELECT * FROM event_log(TABLE(sales_orders_cleaned));

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW latest_update AS SELECT origin.update_id AS id FROM event_log_raw WHERE event_type = 'create_update' ORDER BY timestamp DESC LIMIT 1;

-- COMMAND ----------

-- query for lineage
SELECT
  details:flow_definition.output_dataset as output_dataset,
  details:flow_definition.input_datasets as input_dataset
FROM
  event_log_raw,
  latest_update
WHERE
  event_type = 'flow_definition'
  AND
  origin.update_id = latest_update.id

-- COMMAND ----------

-- query for data quality checks
SELECT
  row_expectations.dataset as dataset,
  row_expectations.name as expectation,
  SUM(row_expectations.passed_records) as passing_records,
  SUM(row_expectations.failed_records) as failing_records
FROM
  (
    SELECT
      explode(
        from_json(
          details :flow_progress :data_quality :expectations,
          "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>"
        )
      ) row_expectations
    FROM
      event_log_raw,
      latest_update
    WHERE
      event_type = 'flow_progress'
      AND origin.update_id = latest_update.id
  )
GROUP BY
  row_expectations.dataset,
  row_expectations.name
