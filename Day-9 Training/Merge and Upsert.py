# Databricks notebook source
employees = [(1, "Scott", "Tiger", 1000.0,
                      "united states"
                     )]
df = spark. \
    createDataFrame(employees,
                    schema="""employee_id INT, first_name STRING,
                    last_name STRING, salary FLOAT, nationality STRING
                    """
                   )
display(df)

# COMMAND ----------

df.createOrReplaceTempView("source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table bronze.employee(
# MAGIC   employee_id int,
# MAGIC   first_name string,
# MAGIC   last_name string,
# MAGIC   salary int,
# MAGIC   nationality string
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from source_view

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bronze.employee

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO bronze.employee as target
# MAGIC USING source_view as source
# MAGIC on target.employee_id=source.employee_id
# MAGIC when Matched
# MAGIC then update set 
# MAGIC     target.first_name=source.first_name,
# MAGIC     target.last_name=source.last_name,
# MAGIC     target.salary=source.salary,
# MAGIC     target.nationality=source.nationality
# MAGIC when not Matched
# MAGIC then insert (employee_id,first_name,last_name,salary,nationality) values(employee_id,first_name,last_name,salary,nationality)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.employee

# COMMAND ----------

employees = [(1, "Scott", "Tiger", 1000.0,
                      "India" ),(2, "John", "Clair", 2000.0,
                      "United Kingdom"
                     )]
df = spark. \
    createDataFrame(employees,
                    schema="""employee_id INT, first_name STRING,
                    last_name STRING, salary FLOAT, nationality STRING
                    """
                   )
df.createOrReplaceTempView("source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC Merge into bronze.employee as target
# MAGIC using source_view as source
# MAGIC on target.employee_id=source.employee_id
# MAGIC when Matched then
# MAGIC Update set
# MAGIC      target.first_name=source.first_name,
# MAGIC     target.last_name=source.last_name,
# MAGIC     target.salary=source.salary,
# MAGIC     target.nationality=source.nationality
# MAGIC when not matched then
# MAGIC insert (employee_id,first_name,last_name,salary,nationality) values(employee_id,first_name,last_name,salary,nationality)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.employee

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history bronze.employee

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended bronze.employee
