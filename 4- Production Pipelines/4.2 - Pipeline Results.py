# Databricks notebook source
files = dbutils.fs.ls("dbfs:/mnt/demo/delta/")
display(files)

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/mnt/demo/delta/system/events")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`dbfs:/mnt/demo/delta/system/events`

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/mnt/demo/dlt/demo_bookstore/tables")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo_bookstore_dlt_db.cn_daily_customer_books

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo_bookstore_dlt_db.fr_daily_customer_books
