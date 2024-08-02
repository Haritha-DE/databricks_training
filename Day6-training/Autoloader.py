# Databricks notebook source
(spark
.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaLocation","dbfs:/mnt/hexawaredatabricks/raw/schemalocation/haritha/autoloader")
.load("dbfs:/mnt/hexawaredatabricks/raw/stream_in/")
.writeStream
.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/haritha/autoloader")
.trigger(once=True)
.table("bronze.autoloader")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.autoloader

# COMMAND ----------

(spark
.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaEvolutionMode","addNewColumns")
.option("cloudFiles.schemaLocation","dbfs:/mnt/hexawaredatabricks/raw/schemalocation/haritha/autoloader")
.load("dbfs:/mnt/hexawaredatabricks/raw/stream_in/")
.writeStream
.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/haritha/autoloader")
.option("mergeSchema",True)
.table("bronze.autoloader")
)

# COMMAND ----------

Auto Loader supports the following modes for schema evolution, which you set in the option cloudFiles.schemaEvolutionMode:

Mode	Behavior on reading new column
addNewColumns (default): Stream fails New columns are added to the schema. Existing columns do not evolve data types.
rescue:	Schema is never evolved and stream does not fail due to schema changes. All new columns are recorded in the rescued data column.
failOnNewColumns:	Stream fails. Stream does not restart unless the provided schema is updated, or the offending data file is removed.
none:	Does not evolve the schema, new columns are ignored, and data is not rescued unless the rescuedDataColumn option is set. Stream does not fail due to schema changes.

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/hexawaredatabricks/raw/stream_in/

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/

# COMMAND ----------

dbutils.fs.cp("dbfs:/mnt/hexawaredatabricks/raw/stream_in/Jan.CSV","dbfs:/mnt/training1231/hexaware/raw/stream_input/")

# COMMAND ----------

dbutils.fs.cp("dbfs:/mnt/hexawaredatabricks/raw/stream_in/Feb.csv","dbfs:/mnt/training1231/hexaware/raw/stream_input/")

# COMMAND ----------

dbutils.fs.cp("dbfs:/mnt/hexawaredatabricks/raw/stream_in/March.csv","dbfs:/mnt/training1231/hexaware/raw/stream_input/")

# COMMAND ----------

dbutils.fs.cp("dbfs:/mnt/hexawaredatabricks/raw/stream_in/April.csv","dbfs:/mnt/training1231/hexaware/raw/stream_input/")

# COMMAND ----------

from pyspark.sql.types import *
userSchema=StructType([
    StructField('Id',IntegerType()),
    StructField('Name',StringType()),
    StructField('Gender',StringType()),
    StructField('Salary',IntegerType()),
    StructField('Country',StringType()),
    StructField('Date',StringType())
])

# COMMAND ----------

(
    spark
    .readStream
    .schema(userSchema)
    .csv("dbfs:/mnt/training1231/hexaware/raw/stream_input/",header=True)
    .writeStream
    .option("checkpointLocation","dbfs:/mnt/training1231/hexaware/raw/checkpoint/stream")
    .trigger(once=True)
    .table("bronze.sample_stream")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.sample_stream

# COMMAND ----------

(
    spark
    .readStream
    .schema(userSchema)
    .csv("dbfs:/mnt/training1231/hexaware/raw/stream_input/",header=True)
    .writeStream
    .option("checkpointLocation","dbfs:/mnt/training1231/hexaware/raw/checkpoint/stream")
    #.trigger(once=True)
    .table("bronze.sample_stream")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.sample_stream

# COMMAND ----------

(
    spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format","csv")
    .option("cloudFiles.inferColumnTypes",True)
    .option("cloudFiles.schemaEvolutionMode","none")
    .option("cloudFiles.schemaLocation","dbfs:/mnt/training1231/hexaware/raw/schemalocation/autoloader")
    .load("dbfs:/mnt/training1231/hexaware/raw/stream_input/")
    .writeStream
    .option("checkpointLocation","dbfs:/mnt/training1231/hexaware/raw/checkpoint/autoloader")
    .option("mergeSchema",True)
    .trigger(once=True)
    .table("bronze.sample_autoloader")

)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.sample_autoloader

# COMMAND ----------

(
    spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format","csv")
    .option("cloudFiles.inferColumnTypes",True)
    .option("cloudFiles.schemaEvolutionMode","rescue")
    .option("cloudFiles.schemaLocation","dbfs:/mnt/training1231/hexaware/raw/schemalocation/autoloader")
    .load("dbfs:/mnt/training1231/hexaware/raw/stream_input/")
    .writeStream
    .option("checkpointLocation","dbfs:/mnt/training1231/hexaware/raw/checkpoint/autoloader")
    .option("mergeSchema",True)
    .trigger(once=True)
    .table("bronze.sample_autoloader")

)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.sample_autoloader

# COMMAND ----------

(
    spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format","csv")
    .option("cloudFiles.inferColumnTypes",True)
    .option("cloudFiles.schemaEvolutionMode","addNewColumns")
    .option("cloudFiles.schemaLocation","dbfs:/mnt/training1231/hexaware/raw/schemalocation/autoloader")
    .load("dbfs:/mnt/training1231/hexaware/raw/stream_input/")
    .writeStream
    .option("checkpointLocation","dbfs:/mnt/training1231/hexaware/raw/checkpoint/autoloader")
    .option("mergeSchema",True)
    .trigger(once=True)
    .table("bronze.sample_autoloader")

)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.sample_autoloader

# COMMAND ----------

(
    spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format","csv")
    .option("cloudFiles.inferColumnTypes",True)
    .option("cloudFiles.schemaEvolutionMode","none")
    .option("cloudFiles.schemaLocation","dbfs:/mnt/training1231/hexaware/raw/schemalocation/autoloader")
    .load("dbfs:/mnt/training1231/hexaware/raw/stream_input/")
    .writeStream
    .option("checkpointLocation","dbfs:/mnt/training1231/hexaware/raw/checkpoint/autoloader")
    .option("mergeSchema",True)
    .trigger(once=True)
    .table("bronze.sample_autoloader")

)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.sample_autoloader
