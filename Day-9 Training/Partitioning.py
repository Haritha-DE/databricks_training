# Databricks notebook source
# MAGIC %run "/Workspace/Users/harithathangilla.99@gmail.com/Day5-training/includes"

# COMMAND ----------

input

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/mnt/hexawaredatabricks/raw/input_files/'

# COMMAND ----------

users_schema=StructType([StructField("Year",IntegerType()),
                         StructField("first_name",StringType()),
                         StructField("County",StringType()),
                         StructField("Gender",StringType()),
                         StructField("Count",IntegerType())
])

# COMMAND ----------

df=spark.read.csv(f"{input}Baby_Names.csv",header=True,schema=users_schema)

# COMMAND ----------

df.count()

# COMMAND ----------

df.groupBy("Year").count().orderBy("Year").display()

# COMMAND ----------

df.write.saveAsTable("bronze.baby_name")

# COMMAND ----------

df.write.save("dbfs:/mnt/hexawaredatabricks/raw/output_files/haritha/baby_names")

# COMMAND ----------

df.write.partitionBy("Year").save("dbfs:/mnt/hexawaredatabricks/raw/output_files/haritha/baby_names_year")

# COMMAND ----------

df.write.partitionBy("Year","Gender").save("dbfs:/mnt/hexawaredatabricks/raw/output_files/haritha/baby_names_year_gender")

# COMMAND ----------


