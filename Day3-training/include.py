# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

input_path='dbfs:/mnt/training1231/hexaware/raw/dataset/formula1/'

# COMMAND ----------

catalog='nydatabricks'
schema='formula1'
