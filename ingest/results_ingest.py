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
        StructField("code", StringType()),
        StructField("dob", StringType()),
        StructField("driverId", IntegerType()),
        StructField("driverRef", StringType()),
        StructField(
            "name",
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

df=spark.read.json("/mnt/bronze/results.json").printSchema()

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
        StructField("constructorId", IntegerType()),
        StructField("driverId", IntegerType()),
        StructField("fastestLap", StringType()),
        StructField("fastestLapSpeed", StringType()),
        StructField("fastestLapTime", StringType()),
        StructField("grid",IntegerType()),
        StructField("laps",IntegerType()),
        StructField("milliseconds", StringType()),
        StructField("number", StringType()),
        StructField("points", StringType()),
        StructField("position", StringType()),
        StructField("positionOrder",IntegerType()),
        StructField("positionText", StringType()),
        StructField("raceId",IntegerType()),
        StructField("rank", StringType()),
        StructField("resultId",IntegerType()),
        StructField("statusId",IntegerType()),
        StructField("time", StringType()),
        ]
)

# COMMAND ----------

df = spark.read.json("/mnt/bronze/constructors.json", schema=input_schema)
df.printSchema()

# COMMAND ----------

# rename column and add a new column with current date
df = (
    df.withColumnRenamed("constructorId", "constructor_id")
    .withColumnRenamed("driverId", "driver_id")
    .withColumnRenamed("raceId", "race_id")
    .withColumnRenamed("resultId", "result_id")
    .withColumnRenamed("statusId", "status_id")
    .withColumnRenamed("grid", "gr_id")
    .withColumn("ingest_dt", lit(current_dt))
)

# COMMAND ----------

df.write.mode("overwrite").parquet("/mnt/silver/drivers")

# COMMAND ----------

df=spark.read.parquet("/mnt/silver/drivers")
df.display()

# COMMAND ----------

df.write.mode('overwrite').parquet('/mnt/silver/results')