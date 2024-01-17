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

# MAGIC %fs ls /FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral

# COMMAND ----------

# MAGIC %fs ls /databricks/cjc/data/

# COMMAND ----------

# os.environ['GDAL_MOSAIC_DBFS'] = "/databricks-mosaic/gdal_3.4.3"          # <-- default is "/FileStore/geospatial/mosaic/gdal/"
# os.environ['GDAL_MOSAIC_FUSE'] = f"/dbfs{os.environ['GDAL_MOSAIC_DBFS']}" # <-- default is "/dbfs/FileStore/geospatial/mosaic/gdal/"

# COMMAND ----------

os.environ['GDAL_MOSAIC_DBFS'] = "/FileStore/geospatial/mosaic/gdal"          # <-- default is "/FileStore/geospatial/mosaic/gdal/"
os.environ['GDAL_MOSAIC_FUSE'] = f"/dbfs{os.environ['GDAL_MOSAIC_DBFS']}" # <-- default is "/dbfs/FileStore/geospatial/mosaic/gdal/"

# COMMAND ----------

file_schema_a_df = spark.read.format("binaryFile").option("pathGlobFilter", "*.nc").load(f"/databricks/cjc/data/")
file_schema_b_df = spark.read.format("binaryFile").option("pathGlobFilter", "*.nc").load(f"/databricks/cjc/data/")

# COMMAND ----------

spark.conf.set("spark.databricks.optimizer.adaptive.enabled", "false")

df2 = (df
    .repartition(20)
    .drop("content")
    .withColumn("subdatasets", mos.rst_subdatasets("path"))
    .withColumn("subdataset", F.map_keys(F.col("subdatasets")).getItem(0))
    .repartition(20, F.col("path"))
    .cache())
    
df2.display()

# COMMAND ----------

# MAGIC %md ## ReTile `subdataset` column to 600x600 pixels

# COMMAND ----------

df3 = df2.withColumn("tiles", mos.rst_retile("subdataset", F.lit(600), F.lit(600)))
df3.display()

# COMMAND ----------

# MAGIC %md ## Render Raster to H3 Results
# MAGIC
# MAGIC > Creates a temp view & renders with Kepler.gl
# MAGIC
# MAGIC Data looks something like the following:
# MAGIC
# MAGIC | h3 | measure |
# MAGIC | --- | ------- |
# MAGIC | 593176490141548543 | 0 |
# MAGIC | 593386771740360703 | 2.0113207547169814 |
# MAGIC | 593308294097928191 | 0 |
# MAGIC | 593825202001936383 | 0.015432098765432098 |
# MAGIC | 593163914477305855 | 2.008650519031142 |
# MAGIC
# MAGIC __Hint: zoom back out once rendered; also, verify the `.contains()` string is actually in the data. Also, this can take a few minutes to run, recommend a few nodes (min. 3 to say 8) in your cluster to speed up processing__

# COMMAND ----------

(df3
  .repartition(20, F.col("tiles"))
  .withColumn("grid_values", mos.rst_rastertogridavg(F.col("subdataset"), F.lit(3)))
  .select("grid_values", "subdataset")
  .select(F.explode(F.col("grid_values")).alias("grid_values"), F.col("subdataset"))
  .select(F.explode(F.col("grid_values")).alias("grid_values"), F.col("subdataset"))
  .where(F.col("subdataset").contains("ct5km_baa-max-7d_v3.1_20220110")) # now `ct5km_baa-max-7d_v3.1_20220110` was `ct5km_baa_max_7d_v3_1_20220101`
  .limit(400000)
  .select(F.col("grid_values").getItem("cellID").alias("h3"), F.col("grid_values").getItem("measure").alias("measure"))
  .createOrReplaceTempView("to_display"))

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC "to_display" "h3" "h3" 400_000

# COMMAND ----------


