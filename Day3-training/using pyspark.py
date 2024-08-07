# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/training1231/hexaware/raw/dataset/formula1/

# COMMAND ----------

df_circuit=spark.read.option('header',True).option('inferschema',True).csv('dbfs:/mnt/training1231/hexaware/raw/dataset/formula1/circuits.csv')

# COMMAND ----------

df_circuit=spark.read.csv('dbfs:/mnt/training1231/hexaware/raw/dataset/formula1/circuits.csv',header=True,inferSchema=True)

# COMMAND ----------

df_circuit.display()

# COMMAND ----------

Extract/Read
 
(Cloud Storage, ADLS/Blob, S3, GCS, Databases: MYSQL, SSMS, PostgreSQL, DW: Snowflake, Redshift, synpase, API:, kafka )
(Format: CSV, json, parquet, orc, delta, avro, excel,text, binary,html,xml)
 
 
Transformation
(pyspark/sql)
 
Load/Write/saving: FILE or TABLE
FORMAT: (Format: CSV, json, parquet, orc, delta, avro, excel,text, binary,html,xml)
where: datalake, DW, lakehouse
 
 
(Recommended)
Load/Write/saving: FILE or TABLE
FORMAT: DELTA
where: lakehouse

# COMMAND ----------

df_circuit.write.saveAsTable("formula1.circuit")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from formula1.circuit
