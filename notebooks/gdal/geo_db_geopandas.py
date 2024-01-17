# Databricks notebook source
# Already installed on the cluster (make sure the DBR matches the mosaic version)
# Need to be using the 13.3LTS for GDAL init script and UC Volumes to work
# %pip install databricks-mosaic=0.4.0
# %pip install databricks-mosaic-gdal==3.4.3
# %pip install geopandas

# COMMAND ----------

# MAGIC %md ### Set up Unity Catalog 

# COMMAND ----------

catalog_name = "cjc"
schema_name = "lom"
# Set up managed catalog called "cjc"
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")

# Set up schema called "lom" within catalog "cjc"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

# COMMAND ----------

# files already extracted to cloud storage location
spark.sql(f"CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.geodb LOCATION 's3://one-env-uc-external-location/shared_location/cjc/geodb';")

# COMMAND ----------

sql(f"use catalog {catalog_name}")
sql(f"use schema {schema_name}")

# COMMAND ----------

a# Create volume to upload init script 
spark.sql(f"create volume if not exists {catalog_name}.{schema_name}.scripts")

# COMMAND ----------

# MAGIC %md ### Configure DatabricksLabs Mosaic Library

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

# DBTITLE 1,Only needs to be run once
# mos.setup_fuse_install(
#   to_fuse_dir="/Volumes/cjc/lom/scripts",
#   with_mosaic_pip=True,
#   with_gdal=True,
#   )

# COMMAND ----------

# Check init script and attach to cluster

# file_path = "/Volumes/cjc/lom/scripts/mosaic-fuse-init.sh"
# f = open(file_path, 'r')
# print(f.read())

# COMMAND ----------

mos.enable_gdal(spark)

# COMMAND ----------

# MAGIC %md ### Load data with geopandas

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, DoubleType

# COMMAND ----------

import geopandas as gpd
import os

# COMMAND ----------

# test reading in one file
df = geopandas.read_file("/Volumes/cjc/lom/geodb/Park.gdb/a00000004.gdbtable")
df.head()

# COMMAND ----------

directory_path = f"/Volumes/{catalog_name}/{schema_name}/geodb/Park.gdb"
files = dbutils.fs.ls(directory_path)
matched_files = [file.name for file in files if "gdbtable" in file.name]

dataframes = {}

for filename in os.listdir(directory_path):
    if filename.endswith(".gdbtable"):
        file_path = os.path.join(directory_path, filename)
        df = gpd.read_file(file_path)
        df_name = filename.split('.')[0]
        dataframes[df_name] = df

# COMMAND ----------

print(dataframes.keys())

# COMMAND ----------

dataframes['a00000002']

# COMMAND ----------

# MAGIC %md ### write out delta tables (not working in batch currently)

# COMMAND ----------

directory_path = f"/Volumes/{catalog_name}/{schema_name}/geodb/Park.gdb"
files = dbutils.fs.ls(directory_path)
matched_files = [file.name for file in files if "gdbtable" in file.name]

for filename in os.listdir(directory_path):
    if filename.endswith(".gdbtable"):
        file_path = os.path.join(directory_path, filename)
        df = gpd.read_file(file_path)
        df_name = filename.split('.')[0]
        
        # schema = StructType([StructField(c, StringType(), True) if t == 'geometry' else StructField(c, DoubleType(), True) for c,t in zip(df.columns, df.dtypes)])

        (spark
         .createDataFrame(df)
         .write
         .format("delta")
         .mode("overwrite")
         .saveAsTable(f"df_name")
        )


# COMMAND ----------


