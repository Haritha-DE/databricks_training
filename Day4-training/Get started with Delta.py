# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/training1231/hexaware/raw/

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema tables

# COMMAND ----------

# MAGIC %sql
# MAGIC create table tables.demo(id int,name string)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into tables.demo values(1,'Hari'),(2,'Laxmi')

# COMMAND ----------

# MAGIC %sql
# MAGIC create table demo2(id int,name string)
# MAGIC Location 'dbfs:/mnt/training1231/hexaware/raw/delta/demo/'

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into demo2 values(1,'Ram'),(2,'Sita')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended demo2

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table demo2
