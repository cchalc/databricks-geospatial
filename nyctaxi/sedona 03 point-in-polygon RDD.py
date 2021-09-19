# Databricks notebook source
# MAGIC %md
# MAGIC # Geospatial Processing on Databricks
# MAGIC 
# MAGIC **Author:** [Derek Yeager](https://www.linkedin.com/in/derekcyeager/) (derek@databricks.com)
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/apache/incubator-sedona/master/docs/image/sedona_logo.png" width="200"/>
# MAGIC 
# MAGIC Example point-in-polygon join implementation using Apache Sedona RDD API and associated configuration options.
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

# MAGIC %md # config

# COMMAND ----------

# increase parallelism to smooth out skew
sqlContext.setConf("spark.sql.shuffle.partitions", "1600")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark import StorageLevel
import geopandas as gpd
import pandas as pd
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import LongType
from shapely.geometry import Point
from shapely.geometry import Polygon

from sedona.register import SedonaRegistrator
from sedona.core.SpatialRDD import SpatialRDD
from sedona.core.SpatialRDD import PointRDD
from sedona.core.SpatialRDD import PolygonRDD
from sedona.core.SpatialRDD import LineStringRDD
from sedona.core.enums import FileDataSplitter
from sedona.utils.adapter import Adapter
from sedona.core.spatialOperator import KNNQuery
from sedona.core.spatialOperator import JoinQuery
from sedona.core.spatialOperator import JoinQueryRaw
from sedona.core.spatialOperator import RangeQuery
from sedona.core.spatialOperator import RangeQueryRaw
from sedona.core.formatMapper.shapefileParser import ShapefileReader
from sedona.core.formatMapper import WkbReader
from sedona.core.formatMapper import WktReader
from sedona.core.formatMapper import GeoJsonReader
from sedona.sql.types import GeometryType
from sedona.core.enums import GridType
from sedona.core.SpatialRDD import RectangleRDD
from sedona.core.enums import IndexType
from sedona.core.geom.envelope import Envelope
from sedona.utils import SedonaKryoRegistrator, KryoSerializer

# COMMAND ----------

spark = SparkSession.\
    builder.\
    appName("Databricks Shell").\
    config("spark.serializer", KryoSerializer.getName).\
    config("spark.kryo.registrator", SedonaKryoRegistrator.getName) .\
    config("spark.jars.packages", "org.apache.sedona:sedona-python-adapter-3.0_2.12:1.0.0-incubating,org.datasyslab:geotools-wrapper:geotools-24.0") .\
    getOrCreate()

# COMMAND ----------

SedonaRegistrator.registerAll(spark)

# COMMAND ----------

# MAGIC %sql USE nyctlc

# COMMAND ----------

# MAGIC %md # Load points and polygons

# COMMAND ----------

points_df = spark.sql("""
  SELECT
    lpep_pickup_datetime,
    Passenger_count,
    Trip_distance,
    Total_amount,
    ST_Point(cast(Pickup_longitude as Decimal(24,20)), cast(Pickup_latitude as Decimal(24,20))) as pickup_point 
  FROM 
    green_tripdata_bronze""") \
  .sample(0.2) \
  .repartition(sc.defaultParallelism * 20) \
  .cache()

# COMMAND ----------

points_rdd = Adapter.toSpatialRdd(points_df, "pickup_point")
#display(Adapter.toDf(points_rdd, spark))

# COMMAND ----------

polygon_df = spark.sql("""
  SELECT 
    zone, 
    ST_GeomFromWKT(the_geom) as geom 
  FROM 
    nyc_taxi_zones_bronze""") \
  .cache()

# COMMAND ----------

polygons_rdd = Adapter.toSpatialRdd(polygon_df, "geom")
#display(Adapter.toDf(polygons_rdd, spark))

# COMMAND ----------

# MAGIC %md # Spatial Partitioning and Indexing

# COMMAND ----------

points_rdd.analyze()

# COMMAND ----------

polygons_rdd.analyze()

# COMMAND ----------

#points_rdd.spatialPartitioning(GridType.KDBTREE)
points_rdd.spatialPartitioning(GridType.QUADTREE)
polygons_rdd.spatialPartitioning(points_rdd.getPartitioner())

# COMMAND ----------

build_on_spatial_partitioned_rdd = True ## Set to TRUE only if run join query
polygons_rdd.buildIndex(IndexType.QUADTREE, build_on_spatial_partitioned_rdd)

# COMMAND ----------

# MAGIC %md # Point in Polygon join operation

# COMMAND ----------

# equivalent to SELECT superhero.name FROM city, superhero WHERE ST_Contains(city.geom, superhero.geom);

using_index = True
considerBoundaryIntersection = True
result = JoinQuery.SpatialJoinQueryFlat(points_rdd, polygons_rdd, using_index, considerBoundaryIntersection)

# COMMAND ----------

# convert RDD to DataFrame
result_df = Adapter.toDf(result, spark).cache()

# fix column names
new_column_name_list = ["polygon", "zone", "point", "lpep_pickup_datetime", "Passenger_count", "Trip_distance", "Total_amount"]
df = result_df.toDF(*new_column_name_list)

df.count()
#display(df)

# COMMAND ----------

# MAGIC %md # Analyze results

# COMMAND ----------

from pyspark.sql.functions import col
display(df.groupBy("zone").count().sort(col("count").desc()))
