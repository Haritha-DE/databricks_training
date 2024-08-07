# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS people10m (
# MAGIC   id INT,
# MAGIC   firstName STRING,
# MAGIC   middleName STRING,
# MAGIC   lastName STRING,
# MAGIC   gender STRING,
# MAGIC   birthDate TIMESTAMP,
# MAGIC   ssn STRING,
# MAGIC   salary INT
# MAGIC ) LOCATION 'dbfs:/mnt/training1231/hexaware/raw/delta/people10m'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe history people10m

# COMMAND ----------

# MAGIC %sql
# MAGIC INsert into people10m values (1,'a','b','z','M','2024-07-30','123',1000)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history people10m

# COMMAND ----------

# MAGIC %sql
# MAGIC INsert into people10m values (2,'a','b','z','M','2024-07-30','123',1000),
# MAGIC                                                  (3,'a','b','z','M','2024-07-30','123',2000)

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from people10m where id= 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from people10m

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from people10m where id= 2

# COMMAND ----------

# MAGIC %sql
# MAGIC update people10m
# MAGIC SET salary=5000
# MAGIC where id= 3

# COMMAND ----------

# MAGIC %sql
# MAGIC INsert into people10m values (5,'a','b','z','M','2024-07-30','123',1000);
# MAGIC INsert into people10m values (8,'a','b','z','M','2024-07-30','123',1000);
# MAGIC INsert into people10m values (9,'a','b','z','M','2024-07-30','123',1000);
# MAGIC INsert into people10m values (27,'a','b','z','M','2024-07-30','123',1000);
# MAGIC INsert into people10m values (25,'a','b','z','M','2024-07-30','123',1000);
# MAGIC INsert into people10m values (21,'a','b','z','M','2024-07-30','123',1000);
# MAGIC INsert into people10m values (21,'a','b','z','M','2024-07-30','123',1000);
# MAGIC INsert into people10m values (222,'a','b','z','M','2024-07-30','123',1000);
# MAGIC INsert into people10m values (236,'a','b','z','M','2024-07-30','123',1000);
# MAGIC INsert into people10m values (26,'a','b','z','M','2024-07-30','123',1000);

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize people10m
# MAGIC zorder by (id)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from people10m

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history people10m

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from people10m timestamp as of '2024-07-30T11:57:18.000+00:00'

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from people10m;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from people10m

# COMMAND ----------

# MAGIC %sql
# MAGIC restore people10m version as of 17

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum people10m

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum people10m retain 0 hours

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled=false

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum people10m retain 0 hours dry run

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum people10m retain 0 hours

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from people10m version as of 3;
