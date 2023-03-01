# Databricks notebook source
import mosaic as mos
mos.enable_mosaic(spark, dbutils)
mos.setup_gdal(spark)

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/geospatial/mosaic/gdal/

# COMMAND ----------

# MAGIC %fs head dbfs:/FileStore/geospatial/mosaic/gdal/mosaic-gdal-init.sh
