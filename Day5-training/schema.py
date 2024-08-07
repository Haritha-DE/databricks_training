# Databricks notebook source
# MAGIC %fs ls dbfs:/databricks-datasets/asa/airlines/

# COMMAND ----------

df=spark.read.csv('dbfs:/databricks-datasets/asa/airlines/',header=True,inferSchema=True)

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %run "/Workspace/Users/harithathangilla.99@gmail.com/Day5-training/includes"

# COMMAND ----------

str_schema="name string,country string,industry string,net_worth_billion double,company string"

# COMMAND ----------

df=spark.read.schema(str_schema).csv(f'{input}',header=True)

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

pyspark_schema=StructType([
    StructField('name',StringType()),
    StructField('country',StringType()),
    StructField('industry',StringType()),
    StructField('net_worth_billion',DoubleType()),
    StructField('company',StringType())
]
)

# COMMAND ----------

df_new=spark.read.schema(pyspark_schema).csv(f"{input}",header=True)

# COMMAND ----------

df_new.display()
