-- Databricks notebook source
CREATE TABLE IF NOT EXISTS example.gold.dim_example_customers (
  CUSTOMER_ID INT,
  CUSTOMER_NAME VARCHAR(255),
  PHONE VARCHAR(255),
  DATE_PARTITION DATE,
  PRIMARY KEY (CUSTOMER_ID)
);

CREATE TABLE IF NOT EXISTS example.gold.dim_example_nations (
  NATION_ID INT,
  NATION_NAME VARCHAR(255),
  DATE_PARTITION DATE,
  PRIMARY KEY (NATION_ID)
);

CREATE TABLE IF NOT EXISTS example.gold.fact_example_orders (
  ORDER_ID INT,
  CUSTOMER_ID INT,
  NATION_ID INT,
  TOTAL_PRICE INT,
  ORDER_DATE DATE,
  DATE_PARTITION DATE,
  PRIMARY KEY (ORDER_ID),
  FOREIGN KEY (CUSTOMER_ID)
  REFERENCES example.gold.dim_example_customers,
  FOREIGN KEY (NATION_ID)
  REFERENCES example.gold.dim_example_nation
);


-- COMMAND ----------

INSERT INTO example.gold.dim_example_customers
SELECT CUSTOMER_ID, 
CUSTOMER_NAME, 
PHONE, 
DATE_PARTITION 
FROM example.silver.example_customers 
WHERE DATE_PARTITION >= current_date();

-- COMMAND ----------

INSERT INTO example.gold.dim_example_nations
SELECT NATION_ID, 
NATION_NAME,  
DATE_PARTITION 
FROM example.silver.example_nations
WHERE DATE_PARTITION >= current_date();

-- COMMAND ----------

INSERT INTO example.gold.fact_example_orders
SELECT 
o.ORDER_ID,
c.CUSTOMER_ID,
c.NATION_ID, 
o.TOTAL_PRICE,
o.ORDER_DATE,
o.DATE_PARTITION 
FROM example.silver.example_orders o
INNER JOIN example.silver.example_customers c
WHERE o.DATE_PARTITION >= current_date();

-- COMMAND ----------


