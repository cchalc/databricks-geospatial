// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC # Analyzing NYC Taxis with [GeoMesa](https://www.geomesa.org)
// MAGIC <p><img src="/files/derek/magellan/nyc.jpg" width="30%"> <img src="/files/derek/magellan/uber.jpg" width="25%"></p>
// MAGIC <p><br></p>
// MAGIC 
// MAGIC 1. Review Spatial Search Problems
// MAGIC 2. Review Spatial Indexing
// MAGIC 3. GeoMesa: Geospatial Vector Analytics
// MAGIC 4. Read NYC Taxi Data from Delta Table (Augmented with GeoMesa)
// MAGIC 5. Load & View NYC Neighborhood Polygon Data (WKT)
// MAGIC 6. Spatial join at scale
// MAGIC 7. What about spatiotemporal queries?
// MAGIC 8. Summary: Analyzing geospatial data at scale using Databricks
// MAGIC 
// MAGIC __<a href="https://docs.databricks.com/libraries.html#install-a-library-on-a-cluster">Install Cluster Libraries</a>:__
// MAGIC 
// MAGIC * GeoMesa Maven Coordinates: `org.locationtech.geomesa:geomesa-spark-jts_2.11:2.3.2`
// MAGIC * LazyLogging Maven Coordinates: `com.typesafe.scala-logging:scala-logging_2.11:3.8.0` (for GeohashUtils)
// MAGIC * Folium PyPI Coordinates: `folium`

// COMMAND ----------

// MAGIC %md ## 1. Spatial Search Problems
// MAGIC 
// MAGIC Spatial data has two fundamental query types:
// MAGIC <p></p>
// MAGIC * K nearest neighbors
// MAGIC * Range queries

// COMMAND ----------

// DBTITLE 1,K nearest neighbors
// MAGIC %md
// MAGIC 
// MAGIC Retrieve the k closest points to a given query point
// MAGIC 
// MAGIC <img src="files/derek/geospark/knn.png" width="50%"></img>

// COMMAND ----------

// DBTITLE 1,Range queries
// MAGIC %md
// MAGIC 
// MAGIC e.g. find all points inside a polygon or circle
// MAGIC 
// MAGIC <img src="files/derek/geospark/rangeq.png" width="50%"></img>

// COMMAND ----------

// DBTITLE 0,Spatial Indexing
// MAGIC %md ## 2. Spatial Indexing
// MAGIC 
// MAGIC Efficient spatial queries at scale requires putting the geometries into a **spatial index**.
// MAGIC 
// MAGIC Querying indexed data with geometric predicate pushdown can dramatically improve performance
// MAGIC 
// MAGIC **R Trees** and **Space Filling Curves** are two common spatial indexing techniques
// MAGIC 
// MAGIC Almost all spatial data structures share the same principle to enable efficient search: branch and bound. It means arranging data in a tree-like structure that allows discarding branches at once if they do not fit our search criteria (data skipping).

// COMMAND ----------

// DBTITLE 1,R Trees
// MAGIC %md
// MAGIC 
// MAGIC R-trees are tree data structures used for spatial access methods, i.e., for indexing multi-dimensional information such as geographical coordinates, rectangles or polygons.  The key idea of the data structure is to **group nearby objects and represent them with their minimum bounding rectangle** in the next higher level of the tree; the "R" in R-tree is for rectangle. Since all objects lie within this bounding rectangle, a query that does not intersect the bounding rectangle also cannot intersect any of the contained objects.
// MAGIC 
// MAGIC <img src="files/derek/geospark/rtree.png" width="50%"></img>

// COMMAND ----------

// DBTITLE 1,Space Filling Curves
// MAGIC %md
// MAGIC 
// MAGIC Space Filling Curves are technique to colocate related information in the same set of files
// MAGIC 
// MAGIC i.e. they allow you to **map multidimensional data to one dimension while maintaining locality**
// MAGIC 
// MAGIC Mapping things down to 1 dimension allows for concepts like range queries, which essentially cuts out scanning entire sections of data since we preserve the locality of the data in higher dimensions with the curve.  
// MAGIC 
// MAGIC <img src="files/derek/geospark/sfc.gif" width="40%"></img>

// COMMAND ----------

// MAGIC %md ## 3. GeoMesa: Geospatial Analytics 
// MAGIC 
// MAGIC There are a number of purpose-built libraries which extend Apache Spark for geospatial analytics. [GeoMesa](https://github.com/locationtech/geomesa) is a a distributed framework especially adept at handling vector data. Others used by Databricks customers include [GeoSpark](http://geospark.datasyslab.org/), [GeoTrellis](https://geotrellis.io/), and [Rasterframes](https://rasterframes.io/). While they often come with a bit of a learning curve, these frameworks often offer multiple language bindings and have much better scaling and performance than non-formalized approaches.  

// COMMAND ----------

// DBTITLE 1,Import GeoMesa Packages
import org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator
import org.locationtech.jts.geom._
import org.locationtech.geomesa.spark.jts._

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import spark.implicits._

// COMMAND ----------

// MAGIC %md _Set serializers for GeoMesa._

// COMMAND ----------

spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
spark.conf.set("spark.kryo.registrator", classOf[GeoMesaSparkKryoRegistrator].getName) 
spark.conf.set("spark.kryoserializer.buffer.max","500m")

// COMMAND ----------

// MAGIC %md _Add GeoMesa User Defined Types (UDT) and Functions (UDF)._

// COMMAND ----------

spark.withJTS

// COMMAND ----------

// MAGIC %md ## 4. Load NYC Taxi Data from Delta Table

// COMMAND ----------

// DBTITLE 1,Dataset comes from the NYC Taxi and Limousine Commission (TLC)
displayHTML("https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page")

// COMMAND ----------

// MAGIC %md _Since we are dealing with 200M points and some skew, want to increase parallelism._

// COMMAND ----------

sqlContext.setConf("spark.sql.shuffle.partitions", "1600") //200 is default
// spark.sharedState.cacheManager.clearCache // cleanup cache as needed

// COMMAND ----------

// MAGIC %md 
// MAGIC * _Get a DataFrame reference to the `nyctaxidelta` table and convert the pickup and dropoff string coordinates into Geometry types using GeoMesa._
// MAGIC * _Also, NYC data should be ~-75 LON and ~40 LAT, so we want to filter out bad points which are present in the provided data._

// COMMAND ----------

// add some min / max checking for NYC area
val lon_min = -80.0
val lon_max = -70.0
val lat_min = 35.0
val lat_max = 45.0

// get a handle on the nyc delta table
val tripsTable = spark.table("nyctaxidelta")

// add geometry and geohash cols
val trips = tripsTable

  // ignore a few irrelevant columns
  .drop("vendorId")
  .drop("rateCodeId")
  .drop("store_fwd")
  .drop("payment_type")
  .drop("neighborhood")

  // filter erroneous points
  .filter($"pickup_longitude" >= lon_min && $"pickup_longitude" <= lon_max)
  .filter($"pickup_latitude" >= lat_min && $"pickup_latitude" <= lat_max)
  .filter($"dropoff_longitude" >= lon_min && $"dropoff_longitude" <= lon_max)
  .filter($"dropoff_latitude" >= lat_min && $"dropoff_latitude" <= lat_max)

  // add the point geometries
  .withColumn("pickupPoint", st_makePoint($"pickup_longitude",$"pickup_latitude"))
  .withColumn("dropoffPoint", st_makePoint($"dropoff_longitude",$"dropoff_latitude"))

  // add geohash
  .withColumn("pickup_geohash_25", st_geoHash($"pickupPoint", 25))
  .withColumn("dropoff_geohash_25", st_geoHash($"dropoffPoint", 25))

// cache on the cluster
.repartition(sc.defaultParallelism * 20)
.cache

// COMMAND ----------

// DBTITLE 1,Dataset contains >200M NYC taxi trips
val numTrips = trips.count

// COMMAND ----------

// DBTITLE 1,View Data (first 1000 records)
display(trips)

// COMMAND ----------

// MAGIC %md _Geohashing at precision 25 allows us to divide the NYC area into ~70 bins._

// COMMAND ----------

trips.select("pickup_geohash_25").distinct.count

// COMMAND ----------

// MAGIC %md _Let's consider the skew for pickups, notice ~110M pickups are in a single geohash (Manhattan)._

// COMMAND ----------

display(
  trips
    .select("pickup_geohash_25")
    .groupBy("pickup_geohash_25")
      .count()
    .orderBy(desc("count")).limit(25)
)

// COMMAND ----------

// MAGIC %md ## 5. Load & View NYC Neighborhood Polygon Data (WKT)
// MAGIC <small></small>
// MAGIC * Data from https://data.cityofnewyork.us/Transportation/NYC-Taxi-Zones/d3c5-ddgc, exported as CSV which has the WKT.
// MAGIC * Data was placed in DBFS at `dbfs:/ml/blogs/geospatial/nyc_taxi_zones.wkt.csv`

// COMMAND ----------

// DBTITLE 1,Use Databricks Built-In CSV Reader
val wktDF = sqlContext.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/ml/blogs/geospatial/nyc_taxi_zones.wkt.csv")

display(wktDF)

// COMMAND ----------

// DBTITLE 1,View Neighborhood Boundaries
// MAGIC %python
// MAGIC 
// MAGIC import folium
// MAGIC import pandas
// MAGIC 
// MAGIC #neighborhoods = 'https://databricks-demo-bucket.s3-us-west-1.amazonaws.com/tlc/neighborhoods/NYCTaxiZones.geojson.txt'
// MAGIC neighborhoods = 'https://databricks-demo-bucket.s3-us-west-1.amazonaws.com/tlc/neighborhoods/neighborhoods.geojson.txt'
// MAGIC 
// MAGIC # create empty map zoomed in on NYC
// MAGIC m = folium.Map(location=[40.7594,-73.9549], zoom_start=11, width=1000, height=600)
// MAGIC 
// MAGIC folium.Choropleth(
// MAGIC     geo_data=neighborhoods,
// MAGIC     name='choropleth',
// MAGIC     fill_color='blue',
// MAGIC     fill_opacity=0.5
// MAGIC ).add_to(m)
// MAGIC 
// MAGIC print("")
// MAGIC displayHTML(m._repr_html_())

// COMMAND ----------

// DBTITLE 1,How many distinct NYC neighborhoods?
val numNeighborhoods = wktDF.distinct.count

// COMMAND ----------

// MAGIC %md ## 6. Let's execute a spatial join at scale!
// MAGIC 
// MAGIC We want to associate each pickup (point) with the appropriate NYC taxi zone (polygon).
// MAGIC 
// MAGIC This _"if point is within polygon"_ query predicate would require comparision of >200M points to ~250 polygons in our example.  (i.e. **worst case 50,000,000,000 expensive comparisons**)
// MAGIC 
// MAGIC To constain joins we are leveraging the precomputed **geohash** information to significantly "prune" the solution space.
// MAGIC 
// MAGIC We then evaluate the **geospatial predicate** `st_contains($"polygon", $"pickupPoint")` (to filter out false positives) 

// COMMAND ----------

// DBTITLE 1,Define UDF to get all GeoHashes Intersecting a Polygon
import scala.util.{Try,Success,Failure}

import org.locationtech.geomesa.utils.geohash.GeohashUtils
import org.locationtech.geomesa.utils.geohash.GeohashUtils._

//This assumes a column which can be coerced to MultiPolygon
val geoHashes = udf( (p: Any) => {
  getUniqueGeohashSubstringsInPolygon(p.asInstanceOf[MultiPolygon],0,5,includeDots=false) match {
  case Success(r) => r.toArray[String]
  case Failure(r) => Array.empty[String]
  }
})

// COMMAND ----------

// DBTITLE 1,Add the `polygon` Geometry Column using GeoMesa + explode intersecting GeoHashes
val neighborhoodsDF = 
  wktDF
    .select("the_geom", "zone")
    .withColumn("polygon", st_geomFromWKT($"the_geom"))
    .withColumn("geohashes",geoHashes($"polygon")).select($"*",explode($"geohashes").alias("geohash"))
    .withColumnRenamed("zone","neighborhood")
    .drop("the_geom")

display(neighborhoodsDF)

// COMMAND ----------

// MAGIC %md _First we figure out `geohash` values with more than 1M pickups.  (used as a "skew hint" to optimize performance below)_

// COMMAND ----------

val pickup_skews = trips
    .select("pickup_geohash_25")
    .groupBy("pickup_geohash_25")
      .count()
    .orderBy(desc("count"))
    .filter($"count" > 1000 * 1000)
    .select("pickup_geohash_25")
    .as[String].collect

// COMMAND ----------

// DBTITLE 1,Spatial Join: predicate = Point within Polygon
val joined = 
  trips
    .hint("skew","pickup_geohash_25", pickup_skews).as("L")
    .join(
      neighborhoodsDF.as("R"),
      // short circuit on geohash and apply geospatial predicate when necessary
      $"L.pickup_geohash_25" === $"R.geohash" && st_contains($"polygon", $"pickupPoint")
    )
    .drop("geohashes")
    .drop("geohash")
    .drop("polygon")

// COMMAND ----------

// DBTITLE 1,View Trips with Neighborhood included
display(joined)

// COMMAND ----------

// MAGIC %md _Cache joined for further use in the notebook (still have the ~200M Points)._

// COMMAND ----------

joined.cache.count

// COMMAND ----------

// DBTITLE 1,View Neighborhood Count Distribution
display(
  joined
    .groupBy('neighborhood)
    .count()
    .orderBy(desc("count"))
)

// COMMAND ----------

// DBTITLE 1,Map Pickup Density by Neighborhood
// MAGIC %python
// MAGIC 
// MAGIC import folium
// MAGIC import pandas
// MAGIC 
// MAGIC neighborhoods = 'https://databricks-demo-bucket.s3-us-west-1.amazonaws.com/tlc/neighborhoods/neighborhoods.geojson.txt'
// MAGIC counts = 'https://databricks-demo-bucket.s3-us-west-1.amazonaws.com/tlc/neighborhoods/neighborhoodCountsFull.csv'
// MAGIC neighborhoodCounts = pandas.read_csv(counts)
// MAGIC 
// MAGIC # define custom bin sizes for a prettier map
// MAGIC bins = [0, 100, 100000, 1000000, 10000000, 37730974]
// MAGIC 
// MAGIC # create empty map zoomed in on NYC
// MAGIC m = folium.Map(location=[40.7594,-73.9549], zoom_start=12, width=1000, height=600)
// MAGIC 
// MAGIC folium.Choropleth(
// MAGIC     geo_data=neighborhoods,
// MAGIC     name='choropleth',
// MAGIC     data=neighborhoodCounts,
// MAGIC     columns=['neighborhood', 'count'],
// MAGIC     key_on='feature.properties.neighborhood',
// MAGIC     fill_color='YlOrRd',
// MAGIC     fill_opacity=0.5,
// MAGIC     line_opacity=0.8,
// MAGIC     bins=[float(x) for x in bins], # folium hack to address bug
// MAGIC     legend_name='Ride Count'
// MAGIC ).add_to(m)
// MAGIC      
// MAGIC displayHTML(m._repr_html_())

// COMMAND ----------

// MAGIC %md ## 7. What about spatiotemporal queries?

// COMMAND ----------

// DBTITLE 1,Convert raw string to Timestamp and parse out month and hour
// convert raw string field to Timestamp
val temporal = joined.select(
  $"neighborhood",
  $"pickup_datetime",
  unix_timestamp($"pickup_datetime", "yyyy-MM-dd HH:mm:ss").cast(TimestampType).as("pickup_timestamp")
)

// parse out hour and month for each trip
val temporalAugmented = temporal.select(
  $"neighborhood",
  $"pickup_datetime",
  $"pickup_timestamp",
  month($"pickup_timestamp").as("pickup_month"),
  hour($"pickup_timestamp").as("pickup_hour")
)

// register temporary table to allow SQL queries
temporalAugmented.createOrReplaceTempView("temporalAugmented")

// COMMAND ----------

// DBTITLE 1,View # of pickups in Midtown by month
// MAGIC %sql
// MAGIC 
// MAGIC SELECT count(*), pickup_month
// MAGIC FROM temporalAugmented
// MAGIC WHERE neighborhood = "Midtown Center"
// MAGIC GROUP BY pickup_month
// MAGIC ORDER BY pickup_month ASC

// COMMAND ----------

// DBTITLE 1,View # of pickups in Midtown by hour of day
// MAGIC %sql
// MAGIC 
// MAGIC SELECT count(*), pickup_hour
// MAGIC FROM temporalAugmented
// MAGIC WHERE neighborhood = "Midtown Center"
// MAGIC GROUP BY pickup_hour
// MAGIC ORDER BY pickup_hour ASC

// COMMAND ----------

// MAGIC %md ## 8. Summary: Analyzing geospatial data at scale using Databricks

// COMMAND ----------

// DBTITLE 0,Summary: Analyzing geospatial data at scale using Databricks
// MAGIC %md
// MAGIC - enables compute-heavy processing at massive scale
// MAGIC - allows pipelining (easily move data from 1 tech to another)
// MAGIC - notebooks & collaboration
// MAGIC - dashboards & visualizations
// MAGIC - elasticity of cloud
// MAGIC - simple cluster management
// MAGIC - advanced analytics (AI/ML use cases)

// COMMAND ----------


