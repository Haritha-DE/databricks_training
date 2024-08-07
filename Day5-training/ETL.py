# Databricks notebook source
# MAGIC %run "/Workspace/Users/harithathangilla.99@gmail.com/Day5-training/includes"

# COMMAND ----------

dbutils.widgets.text("environment"," ")
w=dbutils.widgets.get("environment")

# COMMAND ----------

input

# COMMAND ----------

df=spark.read.csv(f"{input}",header=True,inferSchema=True)

# COMMAND ----------

df1=age_ingestion(df)

# COMMAND ----------

df1.display()

# COMMAND ----------

df1.columns

# COMMAND ----------

new_col=['name',
 'country',
 'industry',
 'net_worth_in_billions',
 'company',
 'ingestion_date']

# COMMAND ----------

df2=df1.toDF(*new_col)

# COMMAND ----------

df2.display()

# COMMAND ----------

#df2.write.mode("overwrite").save(f"{output}haritha/richest")

# COMMAND ----------

df3=df2.withColumn("environment",lit(w))

# COMMAND ----------

#df3.write.mode("overwrite").option("mergeSchema","true").save(f"{output}haritha/richest")

# COMMAND ----------

#df2.createOrReplaceTempView("richest_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from richest_view

# COMMAND ----------

#df2.createGlobalTempView("richest_global_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from global_temp.richest_global_view

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from delta.`dbfs:/mnt/hexawaredatabricks/raw/output_files/haritha/richest`

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists bronze

# COMMAND ----------

df3.write.mode("overwrite").saveAsTable("bronze.richest_bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.richest_bronze
