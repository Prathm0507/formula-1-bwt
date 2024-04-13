# Databricks notebook source
# MAGIC %run ../utils/common_functions

# COMMAND ----------

display(dbutils.fs.ls("/mnt/silver"))

# COMMAND ----------

driver_df=spark.read.parquet('/mnt/silver/drivers/')
constructors_df=spark.read.parquet('/mnt/silver/constructors/')
qualifying_df=spark.read.parquet('/mnt/silver/qualifying/')


# COMMAND ----------

qualifying_df1= qualifying_df.select("driver_id","constructor_id",col("q1").alias("qualifying1"),\
    col("q2").alias("qualifying2"),col("q3").alias("qualifying3"))

driver_df1  =driver_df.withColumn("driver",concat("name.forename",lit(" "),"name.surname"))\
    .select("driver_id","driver_number","driver")

constructors_df1 = constructors_df.select("constructor_id",col("name").alias("team"))

# COMMAND ----------

result_df=qualifying_df1.join(driver_df1,"driver_id","left").join(constructors_df1,"constructor_id","left")
result_df.display()

# COMMAND ----------

result_col_lst=["driver","driver_number","team","qualifying1","qualifying2","qualifying3"]
result_df.select(*result_col_lst).display()
# result_df.display()

# COMMAND ----------

result_df.write.mode("overwrite").parquet("/mnt/gold/qualifying_result")

# COMMAND ----------



# COMMAND ----------

display(spark.read.parquet("/mnt/gold/qualifying_result"))