# Databricks notebook source
# MAGIC %pip install databricks-mosaic

# COMMAND ----------

import mosaic as mos
mos.enable_mosaic(spark, dbutils)
mos.enable_gdal(spark)

# COMMAND ----------

# MAGIC %md ### Mosaic raster function
# MAGIC - https://databrickslabs.github.io/mosaic/api/raster-functions.html?highlight=gdal

# COMMAND ----------


