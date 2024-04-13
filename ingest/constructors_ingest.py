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
        StructField("constructorId", IntegerType()),
        StructField("constructorsRef", StringType()),
        StructField("name", StringType()),
        StructField("nationality", StringType()),
        StructField("url", StringType()),
        
    ]
)

# COMMAND ----------

display(dbutils.fs.ls('/mnt/bronze'))

# COMMAND ----------

display(dbutils.fs.ls('mnt/bronze/constructors.json'))

# COMMAND ----------

df=spark.read.json('/mnt/bronze/constructors.json',schema=input_schema)
df.printSchema()

# COMMAND ----------

# rename column and add a new column with current date
df=df.withColumnRenamed('constructorId','constructor_id').withColumnRenamed('constructorsRef','constructors_ref').withColumn('ingest_dt',lit(current_dt))

# COMMAND ----------

df.write.mode('overwrite').parquet('/mnt/silver/constructors')

# COMMAND ----------

df.display()