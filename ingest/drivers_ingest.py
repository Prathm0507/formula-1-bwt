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
        StructField("driverId", StringType()),
        StructField("dob", StringType()),
        StructField("code", IntegerType()),
        StructField("driverRef", StringType()),
        StructField("name",
            StructType(
                [
                    StructField("forename", StringType()),
                    StructField("surname", StringType()),
                ]
            ),
        ),
        StructField("nationality", StringType()),
        StructField("number", StringType()),
        StructField("url", StringType()),
    ]
)

# COMMAND ----------

df = spark.read.json("/mnt/bronze/drivers.json", schema=input_schema)

# COMMAND ----------

df.withColumn("forename", df["name"]["forename"]).withColumn(
    "surname", df["name"]["surname"]
).drop("name")

# COMMAND ----------

df = spark.read.json("/mnt/bronze/constructors.json", schema=input_schema)
df.printSchema()

# COMMAND ----------

# rename column and add a new column with current date
from pyspark.sql.functions import lit
df = (
    df.withColumnRenamed("driverId", "driver_id")
    .withColumnRenamed("driverRef", "driver_ref")
    .withColumnRenamed("number", "driver_number")
    .withColumn("ingest_dt", lit(current_dt))
)

# COMMAND ----------

df.write.mode("overwrite").parquet("/mnt/silver/drivers")