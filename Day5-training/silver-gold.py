# Databricks notebook source
# MAGIC %sql
# MAGIC create schema if not exists silver

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table silver.richest_silver as
# MAGIC select name,country,industry,net_worth_in_billions,company from bronze.richest_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table gold.country_count as
# MAGIC select country,count(*) as count from silver.richest_silver group by country
# MAGIC order by count desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.country_count
