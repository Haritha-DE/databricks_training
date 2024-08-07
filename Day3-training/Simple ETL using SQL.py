# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/training1231/hexaware/raw/dataset/formula1/

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 1 Extracting data

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema formula1;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table formula1.constructor as 
# MAGIC select * from json.`dbfs:/mnt/training1231/hexaware/raw/dataset/formula1/constructors.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.constructor

# COMMAND ----------

# MAGIC %sql
# MAGIC create table formula1.circuits
# MAGIC as select * from csv.`dbfs:/mnt/training1231/hexaware/raw/dataset/formula1/circuits.csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.circuits
