# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/training1231/hexaware/raw/dataset/formula1/

# COMMAND ----------

df_drivers=spark.read.json('dbfs:/mnt/training1231/hexaware/raw/dataset/formula1/drivers.json')

# COMMAND ----------

df_drivers.display()

# COMMAND ----------

df_drivers.write.saveAsTable("formula1.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.drivers
