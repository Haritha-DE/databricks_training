# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://hexaware@training1231.blob.core.windows.net",
  mount_point = "/mnt/training1231/hexaware",
  extra_configs = {"fs.azure.account.key.training1231.blob.core.windows.net":"key"})

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/training1231/hexaware/raw/

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/mnt/training1231/hexaware/raw/input_data")

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/training1231/hexaware/raw/

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/training1231/hexaware/raw/input_data/")

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/training1231/hexaware/raw/

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/

# COMMAND ----------

dbutils.fs.mv("dbfs:/FileStore/tables/us.csv","dbfs:/mnt/training1231/hexaware/raw/input_data/",False)

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/
