-- Databricks notebook source
CREATE DATABASE  IF NOT EXISTS  formulaone_db;

-- COMMAND ----------

desc database formulaone_db

-- COMMAND ----------

DROP TABLE formulaone_db.pit_stops


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS formulaone_db.pit_stops
(
  raceId integer,
  driverId INTEGER,
  stop INTEGER,
  lap INTEGER,
  time string,
  duration string,
  milliseconds INTEGER
) using csv  LOCATION '/mnt/bronze/pit_stops.json' OPTIONS (multiLine 'true');

-- COMMAND ----------

SELECT * FROM formulaone_db.pit_stops limit 1

-- COMMAND ----------

