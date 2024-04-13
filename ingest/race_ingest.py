# Databricks notebook source
# MAGIC %run ../utils/common_functions 

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    FloatType,DateType
)


input_schema = StructType(
    [
        StructField("raceId", IntegerType()),
        StructField("year", IntegerType()),
        StructField("round", IntegerType()),
        StructField("circuitId", IntegerType()),
        StructField("name", StringType()),
        StructField("date", DateType()),
        StructField("time", StringType()),
        StructField("url", StringType()),
        StructField("fp1_date",DateType()),
        StructField("fp1_time",StringType()),
        StructField("fp2_date",DateType()),
        StructField("fp2_time",StringType()),
        StructField("fp3_date",DateType()),
        StructField("fp3_time",StringType()),
        StructField("quali_date",DateType()),
        StructField("quali_time",StringType()),
        StructField("sprint_date",DateType()),
        StructField("sprint_time",StringType()),
    ]
)

# COMMAND ----------

dbutils.fs.ls('/mnt/bronze')

# COMMAND ----------

df=create_csv_df('dbfs:/mnt/bronze/races.csv',input_schema)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df= df.withColumnRenamed("raceId","race_id").withColumnRenamed(
    "circuitId","circuit_id"
).withColumn('ingest_dt',lit(current_dt))

# COMMAND ----------

df.write.mode("overwrite").parquet("/mnt/silver/race")

# COMMAND ----------

dbutils.fs.ls('/mnt/silver/race')