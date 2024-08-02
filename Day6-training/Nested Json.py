# Databricks notebook source
df=spark.read.json(
"dbfs:/mnt/hexawaredatabricks/raw/input_files/adobe_sample.json"
,multiLine=
True
)

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df1=df\
.withColumn("batters",explode(col("batters.batter")))\
.withColumn("batters_id",col("batters.id"))\
.withColumn("batters_type",col("batters.type"))\
    .drop("batters")\
.withColumn("topping",explode(col("topping")))\
.withColumn("topping_id",col("topping.id"))\
.withColumn("topping_type",col("topping.type"))\
.drop('topping')

# COMMAND ----------

#df1.createOrReplaceTempView("adobe")

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from adobe

# COMMAND ----------

df1.filter((col("batters_type")=='Chocolate') & (col("topping_id") == '5001')).display()
