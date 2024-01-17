# Databricks notebook source
# Already installed on the cluster (make sure the DBR matches the mosaic version)
# Need to be using the 13.3LTS for GDAL init script and UC Volumes to work
# %pip install databricks-mosaic=0.4.0
# %pip install databricks-mosaic-gdal==3.4.3

# COMMAND ----------

# MAGIC %md ### Set up Unity Catalog 

# COMMAND ----------

catalog_name = "cjc"
schema_name = "lom"
# # Set up managed catalog called "cjc"
# spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")

# # Set up schema called "lom" within catalog "cjc"
# spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

# COMMAND ----------

# files already extracted to cloud storage location
# spark.sql(f"CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.geodb LOCATION 's3://one-env-uc-external-location/shared_location/cjc/geodb';")

# COMMAND ----------

sql(f"use catalog {catalog_name}")
sql(f"use schema {schema_name}")

# COMMAND ----------

# Create volume to upload init script 
# spark.sql(f"create volume if not exists {catalog_name}.{schema_name}.scripts")

# COMMAND ----------

# MAGIC %md ### Load data with databricks-mosaic

# COMMAND ----------

# -- import databricks + spark functions

from pyspark.databricks.sql import functions as dbf
from pyspark.sql import functions as F
from pyspark.sql.functions import col

# -- setup mosaic
import mosaic as mos
mos.enable_mosaic(spark, dbutils)

# -- other imports
import os

# COMMAND ----------

# mos.setup_fuse_install(
#   to_fuse_dir="/Volumes/cjc/lom/scripts",
#   with_mosaic_pip=True,
#   with_gdal=True,
#   )

# COMMAND ----------

# file_path = "/Volumes/cjc/lom/scripts/mosaic-fuse-init.sh"
# f = open(file_path, 'r')
# print(f.read())

# COMMAND ----------

mos.enable_gdal(spark)


# COMMAND ----------

df = (
  spark.read.format("geo_db")
  .load("dbfs:/Volumes/cjc/lom/geodb/Park.gdb/a00000001.gdbtable")
)

# COMMAND ----------

# MAGIC %pip install geopandas

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import geopandas
# geopandas.options.io_engine = "pyogrio"


# COMMAND ----------

df = geopandas.read_file("/Volumes/cjc/lom/geodb/Park.gdb/a00000001.gdbtable")

# COMMAND ----------

df.head()

# COMMAND ----------

df = geopandas.read_file("/Volumes/cjc/lom/geodb/Park.gdb/a00000004.gdbtable")
df.head()

# COMMAND ----------


