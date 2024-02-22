-- Databricks notebook source
CREATE TABLE IF NOT EXISTS example.silver.example_customers (
  CUSTOMER_ID INT,
  NATION_ID INT,
  CUSTOMER_NAME VARCHAR(255),
  PHONE VARCHAR(255),
  DATE_PARTITION DATE,
  PRIMARY KEY (CUSTOMER_ID),
  FOREIGN KEY (NATION_ID)
  REFERENCES example.silver.example_nations
);

CREATE TABLE IF NOT EXISTS example.silver.example_nations (
  NATION_ID INT,
  NATION_NAME VARCHAR(255),
  DATE_PARTITION DATE,
  PRIMARY KEY (NATION_ID)
);

CREATE TABLE IF NOT EXISTS example.silver.example_orders (
  ORDER_ID INT,
  CUSTOMER_ID INT,
  TOTAL_PRICE INT,
  ORDER_DATE DATE,
  DATE_PARTITION DATE,
  PRIMARY KEY (ORDER_ID),
  FOREIGN KEY (CUSTOMER_ID)
  REFERENCES example.silver.example_customers
);


-- COMMAND ----------

MERGE INTO example.silver.example_customers AS target
USING (
    SELECT DISTINCT
        c_custkey AS CUSTOMER_ID,
        c_nationkey as NATION_ID,
        UPPER(c_name) AS CUSTOMER_NAME,
        c_phone AS PHONE,
        current_date AS DATE_PARTITION
    FROM example.bronze.example_customers
    WHERE c_custkey IS NOT NULL
) AS source
ON target.CUSTOMER_ID = source.CUSTOMER_ID
WHEN MATCHED THEN
    UPDATE SET
        target.NATION_ID = source.NATION_ID,
        target.CUSTOMER_NAME = source.CUSTOMER_NAME,
        target.PHONE = source.PHONE
WHEN NOT MATCHED THEN
    INSERT (CUSTOMER_ID, NATION_ID, CUSTOMER_NAME, PHONE, DATE_PARTITION)
    VALUES (source.CUSTOMER_ID, source.NATION_ID, source.CUSTOMER_NAME, source.PHONE, source.DATE_PARTITION);


-- COMMAND ----------

MERGE INTO example.silver.example_nations AS target
USING (
    SELECT DISTINCT
        n_nationkey AS NATION_ID,
        n_name AS NATION_NAME,
        current_date AS DATE_PARTITION
    FROM example.bronze.example_nations
    WHERE n_nationkey IS NOT NULL
) AS source
ON target.NATION_ID = source.NATION_ID
WHEN MATCHED THEN
    UPDATE SET
        target.NATION_NAME = source.NATION_NAME

WHEN NOT MATCHED THEN
    INSERT (NATION_ID, NATION_NAME, DATE_PARTITION)
    VALUES (source.NATION_ID, source.NATION_NAME, source.DATE_PARTITION);


-- COMMAND ----------

MERGE INTO example.silver.example_orders AS target
USING (
    SELECT DISTINCT
        o_orderkey AS ORDER_ID,
        o_custkey AS CUSTOMER_ID,
        o_totalprice as TOTAL_PRICE,
        o_orderdate as ORDER_DATE,
        current_date AS DATE_PARTITION
    FROM example.bronze.example_orders
    WHERE o_orderkey IS NOT NULL
) AS source
ON target.ORDER_ID = source.ORDER_ID
AND target.CUSTOMER_ID = source.CUSTOMER_ID

WHEN NOT MATCHED THEN
    INSERT (ORDER_ID, CUSTOMER_ID, TOTAL_PRICE, ORDER_DATE, DATE_PARTITION)
    VALUES (source.ORDER_ID, source.CUSTOMER_ID, source.TOTAL_PRICE, source.ORDER_DATE, source.DATE_PARTITION);

-- COMMAND ----------


