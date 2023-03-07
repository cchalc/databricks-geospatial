# Databricks notebook source
# MAGIC %pip install databricks-mosaic

# COMMAND ----------

# MAGIC %fs ls /FileStore/geospatial/mosaic/gdal/

# COMMAND ----------

import mosaic as mos
mos.enable_mosaic(spark, dbutils)
mos.enable_gdal(spark)

# COMMAND ----------

# MAGIC %md ### Mosaic raster function
# MAGIC - https://databrickslabs.github.io/mosaic/api/raster-functions.html?highlight=gdal

# COMMAND ----------

import os
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md # Read NetCDF File

# COMMAND ----------


