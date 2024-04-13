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
        StructField("deriverId", IntegerType()),
        StructField("stop", IntegerType()),
        StructField("lap", IntegerType()),
        StructField("time", StringType()),
        StructField("duration", StringType()),
        StructField("millisecond",IntegerType()),
        
       
    ]
)

# COMMAND ----------

df=spark.read.json('/mnt/bronze/pit_stops.json',schema=input_schema,multiLine=True) 

# COMMAND ----------

# rename column
df=df.withColumnRenamed('race_id','race_id')
df=df.withColumnRenamed('deriverId','deriver_id')

# define current data value 'YYYY-MM-DD
current_dt=datetime.today().strftime('%y-%m-%d')

# add new column
df=df.withColumn('ingest_dt',lit(current_dt))

# COMMAND ----------

df.write.mode('Overwrite').parquet('/mnt/silver/pit_stops')