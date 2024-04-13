-- Databricks notebook source
CREATE DATABASE  IF NOT EXISTS  formulaone_db;

-- COMMAND ----------

desc database formulaone_db

-- COMMAND ----------

DROP TABLE formulaone_db.circuits

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS formulaone_db.circuits
(
  circuitId integer,
  circuitRef string,
  name string,
  location string,
  country string,
  lat float,
  Ing float,
  alt integer,
  url string
) using csv  LOCATION '/mnt/bronze/circuits.csv' OPTIONS (header 'true');

-- COMMAND ----------

SELECT * FROM formulaone_db.circuits

-- COMMAND ----------

--         StructField("circuitId", IntegerType()),
--         StructField("circuitRef", StringType()),
--         StructField("name", StringType()),
--         StructField("location", StringType()),
--         StructField("country", StringType()),
--         StructField("lat", FloatType()),
--         StructField("Ing", FloatType()),
--         StructField("alt", IntegerType()),
--         StructField("url", StringType()),
--     ]
-- )