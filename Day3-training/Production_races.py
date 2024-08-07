# Databricks notebook source
# MAGIC %run "/Workspace/Users/harithathangilla.99@gmail.com/Day3-training/include"

# COMMAND ----------

# DBTITLE 1,Extract
df_races=spark.read.csv(f"{input_path}races.csv",header=True,inferSchema=True)

# COMMAND ----------

# DBTITLE 1,Transform
df_races1=df_races.withColumnRenamed("raceId","race_id").withColumnRenamed("circuitId","circuit_id")\
    .withColumn('Ingestion_date',current_timestamp()).withColumn('path',input_file_name()).drop("url")

# COMMAND ----------

# DBTITLE 1,Load
df_races1.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.races")

# COMMAND ----------

# DBTITLE 1,Visualize
# MAGIC %sql
# MAGIC select name,count(*) as cnt from formula1.races group by name
