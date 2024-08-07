# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/training1231/hexaware/raw/dataset/formula1/

# COMMAND ----------

df_pit_stops=spark.read.option('multiline',True).json('dbfs:/mnt/training1231/hexaware/raw/dataset/formula1/pit_stops.json')

# COMMAND ----------

df_pit_stops.display()

# COMMAND ----------

df_pit_stops.write.saveAsTable('formula1.pit_stops')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.pit_stops
