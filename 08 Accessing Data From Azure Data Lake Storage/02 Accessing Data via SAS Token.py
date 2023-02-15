# Databricks notebook source
# MAGIC %md
# MAGIC # Accessing Data via SAS Token
# MAGIC 
# MAGIC #### Resources:
# MAGIC * https://learn.microsoft.com/en-us/azure/databricks/external-data/azure-storage#access-azure-data-lake-storage-gen2-or-blob-storage-using-a-sas-token
# MAGIC * https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction-abfs-uri

# COMMAND ----------

# Setting the configuration
# Generate SAS Token with Read and List Permission on container
spark.conf.set("fs.azure.account.auth.type.dbdatafilesstgpj.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.dbdatafilesstgpj.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.dbdatafilesstgpj.dfs.core.windows.net", "sp=racwdl&st=2023-02-15T08:37:00Z&se=2023-02-15T16:37:00Z&spr=https&sv=2021-06-08&sr=c&sig=GKCl0Fyhb1YyXMaMH8U6eSjDPVMQU561hUYyLYitrSQ%3D")

# COMMAND ----------

# Reading data from storage account
spark.read.csv("abfss://countries@dbdatafilesstgpj.dfs.core.windows.net/country_regions.csv", header=True).display()

