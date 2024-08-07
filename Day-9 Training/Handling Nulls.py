# Databricks notebook source
schema=["name","subject","Marks","Attendance"]
student_data=[("John","Math",90,80),("Michael","Science",
70,None), ("David","History",50,40), ("Kelvin","Computer",40,None),("Paul","GEO",None,None), (None,None, None,None),("John","Math",90,80),("John","Math",90,
80
),(
None
,
None
, 
None
,
None
),(
None
,
None
, 
None
,
None
),(
None
,
None
, 
None
,
None
),(
"Michael"
,
"Science"
,
70
,
None
),(
None
,
"Science"
,
90
,
None
),(
" "
,
"NAN"
,
55
,
None
)]

# COMMAND ----------

df=spark.createDataFrame(data=student_data,schema=schema)
df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

df.dropDuplicates(["name"]).display()

# COMMAND ----------

df1=df.dropDuplicates()

# COMMAND ----------

df2=df1.dropna("all")

# COMMAND ----------

df4=df2.dropna(subset="name")
df4.display()

# COMMAND ----------

df3=df2.fillna({"Marks":39,"Attendance":34})
df3.display()

# COMMAND ----------

df3.replace("NAN","IT",subset="subject").display()

# COMMAND ----------

df3.replace("NAN","IT",subset="subject").replace(" ","Robert",subset="name").dropna(subset="name").display()
