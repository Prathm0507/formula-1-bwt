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

dbutils.widgets.text("source_nm","circuits")
source_nm=dbutils.widgets.get("source_nm")

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
        StructField("circuitId", IntegerType()),
        StructField("circuitRef", StringType()),
        StructField("name", StringType()),
        StructField("location", StringType()),
        StructField("country", StringType()),
        StructField("lat", FloatType()),
        StructField("Ing", FloatType()),
        StructField("alt", IntegerType()),
        StructField("url", StringType()),
    ]
)

# COMMAND ----------

# df = spark.read.csv("/mnt/bronze/circuits.csv", header=True,schema=input_schema)
df=create_csv_df("/mnt/bronze/circuits.csv",input_schema)
df.display()

# COMMAND ----------

df.display()
df.printSchema()

# COMMAND ----------

# rename column
df=df.withColumnRenamed('circuitId','circuit_id')
df=df.withColumnRenamed('circuitRef','circuit_ref')

# define current data value 'YYYY-MM-DD
current_dt=datetime.today().strftime('%y-%m-%d')

# add new column
df=df.withColumn('ingest_dt',lit(current_dt))


# COMMAND ----------

df.write.mode('overwrite').parquet('/mnt/silver/{source_nm}')

# COMMAND ----------

spark.read.parquet(f'/mnt/silver/{source_nm}')

# COMMAND ----------

display(dbutils.fs.ls('/mnt/silver'))

# COMMAND ----------

display(dbutils.fs.ls('/mnt/silver/circuits/'))

# COMMAND ----------

df.display()

# COMMAND ----------

dbutils.fs.ls("/mnt/bronze")