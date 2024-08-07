# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/training1231/hexaware/raw/dataset/formula1/

# COMMAND ----------

df_races=spark.read.csv('dbfs:/mnt/training1231/hexaware/raw/dataset/formula1/races.csv',header=True,inferSchema=True)

# COMMAND ----------

df_races.display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_races1=df_races.withColumnRenamed("raceId","race_id").withColumnRenamed("circuitId","circuit_id")\
    .withColumn('Ingestion_date',current_timestamp()).withColumn('path',input_file_name()).drop("url")

# COMMAND ----------

df_races1.display()

# COMMAND ----------

df_races1.write.saveAsTable('formula1.races')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.races
