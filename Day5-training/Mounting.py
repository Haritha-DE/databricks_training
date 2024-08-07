# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://raw@hexawaredatabricks.blob.core.windows.net",
  mount_point = "/mnt/hexawaredatabricks/raw",
  extra_configs = {"fs.azure.account.key.hexawaredatabricks.blob.core.windows.net":"key"})
