# Databricks notebook source
# MAGIC %run ../utils/common_functions 

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    FloatType,
    DateType,
)


input_schema = StructType(
    [
        StructField("constructorId",IntegerType()),
        StructField("driverId",IntegerType()),
        StructField("number", IntegerType()),
        StructField("q1", StringType()),
        StructField("q2",StringType()),
        StructField("q3", StringType()),
        StructField("qualifyId", StringType()),
        StructField("raceId", StringType()),

        ]
    )

# COMMAND ----------

display(dbutils.fs.ls("/mnt/bronze/qualifying"))

# COMMAND ----------

df=spark.read.json("/mnt/bronze/qualifying/qualifying_split_1.json",multiLine=True).printSchema()

# COMMAND ----------

df = spark.read.json("/mnt/bronze/qualifying/qualifying_split_1.json",multiLine=True)
df.printSchema()

# COMMAND ----------

# rename column and add a new column with current date
df = (
    df.withColumnRenamed("constructorId", "constructor_id")
    .withColumnRenamed("driverId", "driver_id")
    .withColumnRenamed("qualifyId", "qualify_id")
    .withColumnRenamed("raceId", "race_id")   
    .withColumn("ingest_dt", lit(current_dt))
)

# COMMAND ----------

df.write.mode("overwrite").parquet("/mnt/silver/qualifying")