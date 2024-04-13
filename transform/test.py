# Databricks notebook source
spark.read.parquet("/mnt/gold/qualifying_result").display()

# COMMAND ----------

from pyspark.sql.functions import regexp_replace
# spark.read.parquet("/mnt/silver/qualifying").select("q1",regexp_replace("q1","\\\\N"," ")).display()
spark.read.parquet("/mnt/gold/qualifying_result").display()

# COMMAND ----------

