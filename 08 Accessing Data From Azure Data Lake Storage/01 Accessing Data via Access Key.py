# Databricks notebook source
# MAGIC %md
# MAGIC # Accessing Data via Access Key
# MAGIC 
# MAGIC #### Resources:
# MAGIC * https://learn.microsoft.com/en-us/azure/databricks/external-data/azure-storage#--access-azure-data-lake-storage-gen2-or-blob-storage-using-the-account-key
# MAGIC * https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction-abfs-uri

# COMMAND ----------

# Paste in your account key in the second argument
spark.conf.set(
    "fs.azure.account.key.dbdatafilesstgpj.dfs.core.windows.net",
    "eq4XrDdcq+Y9R9L1vxWZ1gq0yMaR74fKhd1Kvbz4++vCBY3hmjITkGVxF3vqSfBhOXcy/do2lyzE+AStUF+ENw==")

# COMMAND ----------

# Reading data from the storage account
countriesdf = spark.read.csv("abfss://countries@dbdatafilesstgpj.dfs.core.windows.net/countries.csv", header=True)

# COMMAND ----------

# Reading data from the storage account
regionsdf = spark.read.csv("abfss://countries@dbdatafilesstgpj.dfs.core.windows.net/country_regions.csv", header=True)

# COMMAND ----------

regions.display()

# COMMAND ----------

countries.display()
