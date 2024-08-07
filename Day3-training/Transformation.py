# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/training1231/hexaware/raw/dataset/formula1/

# COMMAND ----------

df_circuit=spark.read.csv("dbfs:/mnt/training1231/hexaware/raw/dataset/formula1/circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

df_circuit.display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_circuit.select(col("circuitId"),"name",df_circuit["location"]).display()

# COMMAND ----------

df_circuit.select(concat("location",lit(" "),"country").alias('concat')).display()

# COMMAND ----------

df_circuit.withColumnRenamed("circuitId","circuit_id").withColumnRenamed("circuitRef","circuit_ref").display()

# COMMAND ----------

df_circuit.withColumn("Ingestion_date",current_date()).withColumn("path",input_file_name())\
    .drop("url").display()
