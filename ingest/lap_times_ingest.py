# Databricks notebook source
# MAGIC %md
# MAGIC 1.read csv file
# MAGIC
# MAGIC 2.apply schema for it
# MAGIC
# MAGIC 3.rename and remove column based on the requirement

# COMMAND ----------

# MAGIC %run ../utils/common_functions 

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    FloatType,
)


input_schema = StructType(
    [
        StructField("raceId", IntegerType()),
        StructField("driverId",IntegerType()),
        StructField("lap", IntegerType()),
        StructField("position", IntegerType()),
        StructField("time", StringType()),
        StructField("millisecond", IntegerType()),
       
    ]
)

# COMMAND ----------

df = spark.read.csv("/mnt/bronze/lap_times",input_schema,False)

# COMMAND ----------

# rename column
df=df.withColumnRenamed('raceId','race_id')
df=df.withColumnRenamed('driverId','driver_id')

# define current data value 'YYYY-MM-DD
current_dt=datetime.today().strftime('%y-%m-%d')

# add new column
df=df.withColumn('ingest_dt',lit(current_dt))


# COMMAND ----------

df.write.mode('overwrite').parquet('/mnt/silver/lap_times')