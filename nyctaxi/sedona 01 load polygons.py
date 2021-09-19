# Databricks notebook source
# MAGIC %md
# MAGIC # Load Polygon Data
# MAGIC 
# MAGIC **Author:** [Derek Yeager](https://www.linkedin.com/in/derekcyeager/) (derek@databricks.com)
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/apache/incubator-sedona/master/docs/image/sedona_logo.png" width="200"/>
# MAGIC 
# MAGIC Load polygon boundary data for downstream analysis
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

# MAGIC %md # Read WKT (CSV) to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC USE nyctlc

# COMMAND ----------

wktDF = sqlContext.read.format("csv") \
  .option("header", "true") \
  .option("inferSchema", "true") \
  .load("/ml/blogs/geospatial/nyc_taxi_zones.wkt.csv")

display(wktDF)

# COMMAND ----------

numNeighborhoods = wktDF.count()
print(numNeighborhoods)

# COMMAND ----------

wktDF.write \
  .format("delta") \
  .mode("overwrite") \
  .save("/home/derek.yeager@databricks.com/nyctaxi/taxi_zones")

# COMMAND ----------

spark.sql("CREATE TABLE nyc_taxi_zones_bronze USING DELTA LOCATION '" + "/home/derek.yeager@databricks.com/nyctaxi/taxi_zones" + "'")

# COMMAND ----------

example = spark.sql("SELECT * FROM nyc_taxi_zones_bronze LIMIT 1")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM nyc_taxi_zones_bronze LIMIT 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   borough,
# MAGIC   count(*) as count
# MAGIC FROM
# MAGIC   nyc_taxi_zones_bronze
# MAGIC GROUP BY
# MAGIC   borough
# MAGIC ORDER BY
# MAGIC   count DESC

# COMMAND ----------



# COMMAND ----------


