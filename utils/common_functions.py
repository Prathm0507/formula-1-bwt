# Databricks notebook source
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    FloatType,DateType,datetime,
)
from datetime import datetime
from pyspark.sql.functions import lit,col,concat,regexp_replace

# COMMAND ----------

def create_csv_df(input_location,schema,header_status=True):
    """
    This function is used for creating a spark dataframe on csv file location
    :input_location: provide input csv file location 
    :schema:provide input schema
    :return spark dataframe

    """
    return spark.read.csv(input_location, header=header_status,schema=schema)



# COMMAND ----------

# define current data value 'YYYY-MM-DD
current_dt=datetime.today().strftime('%y-%m-%d')