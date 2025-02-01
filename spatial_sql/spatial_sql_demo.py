# Databricks notebook source
# MAGIC %md
# MAGIC # Product-native spatial SQL functions

# COMMAND ----------

# MAGIC %pip install geopandas folium mapclassify

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enabling the functions

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.geo.st.enabled=true

# COMMAND ----------

# MAGIC %md
# MAGIC ## Construct a geometry

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW pois AS
# MAGIC SELECT
# MAGIC   "General Wolfe" AS poi_name, 
# MAGIC   ST_ASTEXT(ST_POINT(-0.000767, 51.477798)) AS geom;
# MAGIC SELECT * FROM pois;

# COMMAND ----------

# MAGIC %md
# MAGIC See also:
# MAGIC - st_makeline
# MAGIC - st_makepolygon
# MAGIC - st_geomfromtext
# MAGIC - st_geomfromwkb
# MAGIC - st_geomfromgeojson

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick inspection of our data

# COMMAND ----------

import geopandas as gpd
gs = gpd.GeoSeries.from_wkt(_sqldf.toPandas()["geom"], crs=4326)
pdf = _sqldf.toPandas()
pdf["geometry"] = gs
gdf = gpd.GeoDataFrame(pdf, geometry="geometry")
gdf.explore()

# COMMAND ----------

# MAGIC %md
# MAGIC ![](/Workspace/Users/stuart.lynn@databricks.com/geospatial/product-spatial-SQL/assets/wolfe-statue.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coordinate reprojection

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW areas AS
# MAGIC WITH bng_poly AS (
# MAGIC   SELECT "Polygon ((539080.74751770845614374 177832.91306236910168082, 538767.14163828617893159 177684.27982682932633907, 538727.31745586439501494 177673.18859504663851112, 538621.84594145440496504 177616.95230604690732434, 538490.54813365358859301 177526.66315907711395994, 538461.83612446254119277 177475.85392566624796018, 538466.63019655959215015 177422.62076200800947845, 538477.19970560283400118 177402.8968720713746734, 538515.22874738450627774 177357.2380391139886342, 538559.66927617788314819 177321.75953704561106861, 538577.28611368732526898 177288.88596884300932288, 538600.60744349961169064 177169.44750937586650252, 538625.09037635033018887 177130.08916657301597297, 538648.84648620593361557 177117.39449425134807825, 538700.42835317435674369 177065.43241473636589944, 538749.9418425892945379 176966.71622413786826655, 538798.00173944456037134 176921.32852166821248829, 538822.66879277874249965 176875.30273452715482563, 538902.44626043341122568 176770.73795371776213869, 538981.31591152853798121 176699.5043591340072453, 539001.45930460619274527 176696.71848380187293515, 539100.53612874797545373 176742.78883285738993436, 539180.19766917137894779 176764.98013099154923111, 539311.5966025177622214 176851.966067130852025, 539377.52282120834570378 176887.12700300174765289, 539480.11801618535537273 176926.62874431553063914, 539529.15005440777167678 176967.99971074820496142, 539575.38804935966618359 176989.28076505078934133, 539581.70609962672460824 177002.79634332819841802, 539647.44598334014881402 177044.62610658182529733, 539653.76379332377109677 177058.14164582575904205, 539649.6901807285612449 177084.71425135689787567, 539624.83299735363107175 177137.40057581866858527, 539555.43555313709657639 177228.89047401363495737, 539520.28041602007579058 177291.29953734739683568, 539471.85091075743548572 177350.00893362716306001, 539458.30143866187427193 177356.30838098999811336, 539392.25132515386212617 177447.88782484497642145, 539289.26398220960982144 177545.12575660482980311, 539223.31043143896386027 177633.37107035593362525, 539178.77629905252251774 177672.17467219801619649, 539140.10464662709273398 177741.15433048742124811, 539123.03277574293315411 177754.02790662919869646, 539074.24719945201650262 177826.0654000723734498, 539080.74751770845614374 177832.91306236910168082))" AS geom_27700)
# MAGIC SELECT
# MAGIC   "Greenwich Park" AS area_name,
# MAGIC   geom_27700,
# MAGIC   ST_ASTEXT(
# MAGIC     ST_TRANSFORM(
# MAGIC       ST_SETSRID(
# MAGIC         ST_GEOMFROMTEXT(geom_27700),
# MAGIC       27700),
# MAGIC     4326)
# MAGIC   ) AS geom_4326
# MAGIC FROM
# MAGIC   bng_poly;
# MAGIC
# MAGIC SELECT * FROM areas;

# COMMAND ----------

gs = gpd.GeoSeries.from_wkt(_sqldf.toPandas()["geom_4326"], crs=4326)
pdf = _sqldf.toPandas()
pdf["geometry"] = gs
gdf = gpd.GeoDataFrame(pdf, geometry="geometry")
gdf.explore()

# COMMAND ----------

# MAGIC %md
# MAGIC # Compute spatial measures

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   area_name,
# MAGIC   ST_AREA(geom_27700) AS geom_area_m, --cartesian area, metres
# MAGIC   ST_AREA(geom_4326) AS geom_area_deg, --cartesian area, degrees
# MAGIC   ST_GEOGAREA(geom_4326) AS geog_area_m --geodesic area (assumes WGS84)
# MAGIC FROM
# MAGIC   areas

# COMMAND ----------

# MAGIC %md
# MAGIC ## Point-in-poly join

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   a.area_name,
# MAGIC   p.poi_name,
# MAGIC   ST_CONTAINS(a.geom_4326, p.geom) AS is_contained
# MAGIC FROM areas a , pois p

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply an index to the data

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW indexed_poi AS
# MAGIC SELECT
# MAGIC   *,
# MAGIC   H3_POINTASH3(geom, 9) AS h3
# MAGIC FROM pois;
# MAGIC
# MAGIC SELECT * FROM indexed_poi;

# COMMAND ----------

import pyspark.databricks.sql.functions as DBF

indexed_point_pdf = (
  spark.table("indexed_poi")
  .withColumn("h3_geom", DBF.h3_boundaryaswkt("h3"))
  ).toPandas()
indexed_point_pdf["geometry"] = gpd.GeoSeries.from_wkt(indexed_point_pdf["geom"], crs=4326)
indexed_point_pdf["h3_geometry"] = gpd.GeoSeries.from_wkt(indexed_point_pdf["h3_geom"], crs=4326)
m = gpd.GeoDataFrame(indexed_point_pdf, geometry="geometry").explore()
gpd.GeoDataFrame(indexed_point_pdf, geometry="h3_geometry").explore(m=m)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW h3_tessellation AS
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     EXPLODE(H3_TESSELLATEASWKB(geom_4326, 9)) AS h3
# MAGIC   FROM areas;
# MAGIC
# MAGIC SELECT * FROM h3_tessellation;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW indexed_areas AS
# MAGIC SELECT
# MAGIC   * EXCEPT (h3),
# MAGIC   h3.*,
# MAGIC   ST_ASTEXT(ST_GEOMFROMWKB(h3.chip)) AS chip_wkt
# MAGIC FROM h3_tessellation;
# MAGIC SELECT * FROM indexed_areas;

# COMMAND ----------

indexed_areas_pdf = (
  spark.table("indexed_areas").select("geom_4326", "chip_wkt")
  ).toPandas()
indexed_areas_pdf["h3_geometry"] = gpd.GeoSeries.from_wkt(indexed_areas_pdf["chip_wkt"], crs=4326)
gpd.GeoDataFrame(indexed_areas_pdf, geometry="h3_geometry").explore()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Indexed join

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   a.area_name,
# MAGIC   p.poi_name
# MAGIC FROM indexed_areas a
# MAGIC   INNER JOIN indexed_poi p
# MAGIC   ON a.cellid = p.h3
# MAGIC WHERE
# MAGIC   a.core OR ST_CONTAINS(a.chip, p.geom)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Doing something useful: how much time do people spend in the park?

# COMMAND ----------

# MAGIC %md
# MAGIC We'll analyse a couple of Strava activities and determine how long this person spends enjoying the view over the river on their runs.

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.databricks.sql.functions as DBF

run_track = "file:/Workspace/Users/stuart.lynn@databricks.com/geospatial/product-spatial-SQL/strava"

(
  spark.read
  .csv(run_track, header=True)
  .withColumnRenamed("track_se_1", "track_seg_point_id")
  .select("WKT", "track_seg_point_id", "ele", "time")
  .where("!st_isempty(WKT)")
  .withColumn("user", F.lit("uuʎ˥ ʇɹɐnʇS"))
  .withColumn("time", F.to_timestamp("time", "yyyy/MM/dd HH:mm:ss.SSS"))
  .withColumn("date", F.date_trunc("day", "time"))
  .withColumn("h3", DBF.h3_pointash3("WKT", 9))
  ).createOrReplaceTempView("run_track")

spark.table("run_track").display()

# COMMAND ----------

run_track_pdf = spark.table("run_track").sample(fraction=0.5).toPandas()
gs = gpd.GeoSeries.from_wkt(run_track_pdf["WKT"], crs=4326)
run_track_pdf["geometry"] = gs
gdf = gpd.GeoDataFrame(run_track_pdf, geometry="geometry")
gdf.explore(column="date")

# COMMAND ----------

run_track_pdf = (
  spark.table("run_track")
  .withColumn("h3_geom", DBF.h3_boundaryaswkt("h3"))
  .select("h3", "h3_geom", "date")
  .distinct()
  ).toPandas()
gs = gpd.GeoSeries.from_wkt(run_track_pdf["h3_geom"], crs=4326)
run_track_pdf["geometry"] = gs
gdf = gpd.GeoDataFrame(run_track_pdf, geometry="geometry")
gdf.explore(column="date")

# COMMAND ----------

events_in_aoi_df = (
  spark.table("run_track").alias("t")
  .join(
    other=spark.table("indexed_areas").alias("a"),
    on=F.col("t.h3")==F.col("a.cellid"),
    how="inner"
    )
  .where(F.col("a.core") | F.expr("st_contains(a.chip, t.WKT)"))
  .select("user", "date", "time", "track_seg_point_id", "ele", "WKT")
)

events_in_aoi_df.display()

# COMMAND ----------

events_pdf = events_in_aoi_df.toPandas()
gs = gpd.GeoSeries.from_wkt(events_pdf["WKT"], crs=4326)
events_pdf["geometry"] = gs
gdf = gpd.GeoDataFrame(events_pdf, geometry="geometry")
gdf.explore(column="date")

# COMMAND ----------

(
    events_in_aoi_df.groupBy("user", "date")
    .agg(F.min("time").alias("in_time"), F.max("time").alias("out_time"))
    .withColumn(
        "dwell",
        F.timestamp_diff(start=F.col("in_time"), end=F.col("out_time"), unit="SECOND"),
    )
).display()
