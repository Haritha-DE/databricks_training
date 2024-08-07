# Databricks notebook source
input="dbfs:/mnt/hexawaredatabricks/raw/input_files/"
output="dbfs:/mnt/hexawaredatabricks/raw/output_files/"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

def age_ingestion(a):
    b=a.withColumn("ingestion_date",current_timestamp())
    return b

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from richest_view

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from global_temp.richest_global_view
