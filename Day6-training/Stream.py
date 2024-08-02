# Databricks notebook source
from pyspark.sql.types import *
users_schema=StructType([StructField("Id", IntegerType()),
                         StructField("Name", StringType()),
                         StructField("Gender", StringType()),
                         StructField("Salary", IntegerType()),
                         StructField("Country", StringType()),
                         StructField("Date", StringType())
])
 

# COMMAND ----------

# MAGIC %sql
# MAGIC use bronze;
# MAGIC create or replace table bronze.stream;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table bronze.stream;

# COMMAND ----------

(
    spark
    .readStream
    .schema(users_schema)
    .csv("dbfs:/mnt/hexawaredatabricks/raw/stream_in/",header=True)
    .writeStream
    .option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/haritha/stream")
    .trigger(once=True)
    .table("nydatabricks.bronze.streamm")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.streamm
