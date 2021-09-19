# Databricks notebook source
# MAGIC %md
# MAGIC # Geospatial Processing on Databricks
# MAGIC 
# MAGIC **Author:** [Derek Yeager](https://www.linkedin.com/in/derekcyeager/) (derek@databricks.com)
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/apache/incubator-sedona/master/docs/image/sedona_logo.png" width="200"/>
# MAGIC 
# MAGIC Example point-in-polygon join implementation using Apache Sedona SQL API.
# MAGIC 
# MAGIC Ensure you are using a DBR with Spark 3.0, Java 1.8, Scala 2.12, and Python 3.7+ (tested on 7.3 LTS)
# MAGIC 
# MAGIC Link the following libraries to your cluster:
# MAGIC 
# MAGIC - **Maven Central**
# MAGIC     - `org.apache.sedona:sedona-python-adapter-3.0_2.12:1.0.0-incubating`
# MAGIC     - `org.datasyslab:geotools-wrapper:geotools-24.0`
# MAGIC - **PyPi**
# MAGIC     - `apache-sedona`
# MAGIC     - `geopandas==0.6.0`

# COMMAND ----------

# MAGIC %md # Config

# COMMAND ----------

sqlContext.setConf("spark.sql.shuffle.partitions", "1600")
#spark.conf.set("spark.sql.files.maxPartitionBytes", 1024 * 1024 * 16) # 16mb

# COMMAND ----------

import os

from pyspark.sql import SparkSession

from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer

# COMMAND ----------

spark = SparkSession. \
    builder. \
    appName('Databricks Shell'). \
    config("spark.serializer", KryoSerializer.getName). \
    config("spark.kryo.registrator", SedonaKryoRegistrator.getName). \
    config('spark.jars.packages',
           'org.apache.sedona:sedona-python-adapter-3.0_2.12:1.0.0-incubating,org.datasyslab:geotools-wrapper:geotools-24.0'). \
    getOrCreate()

# COMMAND ----------

SedonaRegistrator.registerAll(spark)

# COMMAND ----------

# MAGIC %sql USE nyctlc

# COMMAND ----------

# MAGIC %md # Load Points

# COMMAND ----------

points_df = spark.sql("""
  SELECT
    lpep_pickup_datetime,
    Passenger_count,
    Trip_distance,
    Tolls_amount,
    ST_Point(cast(Pickup_longitude as Decimal(24,20)), cast(Pickup_latitude as Decimal(24,20))) as pickup_point 
  FROM 
    green_tripdata_bronze""") \
  .sample(0.1) \
  .repartition(sc.defaultParallelism * 20) \
  .cache()
  
num_points_in_sample = points_df.count()

# COMMAND ----------

# MAGIC %md # Load Polygons

# COMMAND ----------

polygon_df = spark.sql("""
  SELECT 
    zone, 
    ST_GeomFromWKT(the_geom) as geom 
  FROM 
    nyc_taxi_zones_bronze""") \
  .cache()

num_polygons = polygon_df.count()

# COMMAND ----------

print("points: ", num_points_in_sample)
print("polygons: ", num_polygons)
print("worst case lookups: ", num_points_in_sample * num_polygons)

# COMMAND ----------

points_df.createOrReplaceTempView("points")
polygon_df.createOrReplaceTempView("polygons")

# COMMAND ----------

# MAGIC %md # Point in Polygon join operation

# COMMAND ----------

point_in_polygon = spark.sql("""
  SELECT 
    lpep_pickup_datetime,
    Passenger_count,
    Trip_distance,
    Tolls_amount,
    points.pickup_point, 
    polygons.zone 
  FROM 
    points, polygons 
  WHERE 
    ST_Contains(polygons.geom, points.pickup_point)"""
)

# COMMAND ----------

point_in_polygon.explain("formatted")

# COMMAND ----------

point_in_polygon.cache()

# COMMAND ----------

num_matched = point_in_polygon.count()
num_unmatched = num_points_in_sample - num_matched
print("Number of points matched: ", num_matched)
print("Number of points unmatched: ", num_unmatched)

# COMMAND ----------

display(point_in_polygon)

# COMMAND ----------

# MAGIC %md # Analyze results

# COMMAND ----------

from pyspark.sql.functions import col
display(point_in_polygon.groupBy("zone").count().sort(col("count").desc()))
