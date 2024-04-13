# Databricks notebook source
display(dbutils.fs.ls('/mnt/silver/results/'))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`/mnt/silver/results`

# COMMAND ----------


race_df=spark.read.parquet("/mnt/silver/race")

# COMMAND ----------

from pyspark.sql.functions import col,year
race_df=race_df.withColumn("year_dt",year(col("date"))).select("race_id","year_dt")

# COMMAND ----------

result_df = spark.sql("select * from  parquet.`/mnt/silver/results`").select ("driver_id","race_id","constructor_id","points","position","positionText")


# COMMAND ----------

race_result_df = result_df.join(race_df,'race_id','left')
race_result_df.display()

# COMMAND ----------

from pyspark.sql.functions import count, col, when,sum,lit,rank,desc
from pyspark.sql import Window
driverstands_df=race_result_df.groupBy("year_dt", "driver_id").agg(sum('points').alias("total_points"), count(when(col("position") == 1,1)).alias('win'))

driverstands_df=driverstands_df.withColumn("driverstanding_id",rank().over(Window.partitionBy('year_dt').orderBy(desc('total_points'),desc('win'))))

# COMMAND ----------

from pyspark.sql.functions import concat
driver_df=spark.read.parquet('/mnt/silver/drivers').select('driver_id',concat("name.forename","name.surname").alias("driver_nm"))

# COMMAND ----------

driverstands_df=driverstands_df.join(driver_df,'driver_id','inner')
driverstands_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW formulaone_db.results_view AS 
# MAGIC select
# MAGIC  *
# MAGIC from
# MAGIC parquet.`/mnt/silver/results`

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,driver_id,points,position,positionText from formulaone_db.results_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,driver_id,points,position,positionText, max(points)over (partition by race_id order by race_id desc) as wins from formulaone_db.results_view

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC extended formulaone_db.results_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formulaone_db.results_view

# COMMAND ----------

display(dbutils.fs.ls("/mnt/silver/results/"))