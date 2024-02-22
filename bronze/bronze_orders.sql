-- Databricks notebook source
CREATE TABLE IF NOT EXISTS example.bronze.example_customers
USING DELTA AS
SELECT DISTINCT c_custkey, c_name, c_phone, c_nationkey
FROM samples.tpch.customer;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS example.bronze.example_orders 
USING DELTA AS
SELECT DISTINCT o_orderkey, 
o_custkey, 
o_totalprice,
o_orderdate 
FROM samples.tpch.orders;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS example.bronze.example_nations 
USING DELTA AS
SELECT DISTINCT n_nationkey, 
n_name
FROM samples.tpch.nation;

-- COMMAND ----------


