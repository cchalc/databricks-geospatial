// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC # Geospatial Processing on Databricks
// MAGIC 
// MAGIC **Author:** [Derek Yeager](https://www.linkedin.com/in/derekcyeager/) (derek@databricks.com)
// MAGIC 
// MAGIC <img src="https://1fykyq3mdn5r21tpna3wkdyi-wpengine.netdna-ssl.com/wp-content/uploads/2018/06/image12.png" alt="drawing" width="200"/>
// MAGIC 
// MAGIC The New York City Taxi and Limousine Commission (TLC) provides a [dataset](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) of trips taken by taxis and for-hire vehicles in New York City.  AWS hosts this dataset in an S3 bucket in the `us-east-1` region (more information [here]( https://registry.opendata.aws/nyc-tlc-trip-records-pds/)).
// MAGIC 
// MAGIC Within this dataset, there are records for Yellow taxis, Green taxis, and for-hire vehicles (e.g. Uber, Lyft) with lat/lon associated.  
// MAGIC 
// MAGIC Data details:
// MAGIC - Yellow trip data from January 2009 through June 2016
// MAGIC - Green trip data from August 2013 through June 2016
// MAGIC - FHV trip data (does not include coordinates)
// MAGIC 
// MAGIC ***IMPORTANT***: Be sure to edit `BASE_PATH_DBFS` in `Cmd 2` line #2 before running this notebook.

// COMMAND ----------

// MAGIC %md
// MAGIC # Green Trip Data

// COMMAND ----------

// DBTITLE 1,Configure Paths
// modify this accordingly
val BASE_PATH_DBFS = "/home/christopher.chalcraft@databricks.com/nyctaxi/green_tripdata"

//val RAW_PATH = BASE_PATH_DBFS + "/raw"
//val SCHEMA_V1_RAW_PATH = RAW_PATH + "/schema_v1"
//val SCHEMA_V2_RAW_PATH = RAW_PATH + "/schema_v2"
val SCHEMA_V1_RAW_PATH = BASE_PATH_DBFS + "/schema_v1"
val SCHEMA_V2_RAW_PATH = BASE_PATH_DBFS + "/schema_v2"
val BRONZE_PATH = "dbfs:" + BASE_PATH_DBFS + "/bronze"

// COMMAND ----------

// DBTITLE 1,Write out some paths (used in Cmd 7 & 9)
import java.io.PrintWriter
new PrintWriter("schema_v1_path.txt") { try {write("/dbfs"+SCHEMA_V1_RAW_PATH); } finally {close} }
new PrintWriter("schema_v2_path.txt") { try {write("/dbfs"+SCHEMA_V2_RAW_PATH); } finally {close} }

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ***Note:*** Beginning `2015-01.csv` the green_tripdata schema changed, and a new column `improvement_surcharge` was added.  We will handle this automatically, using the schema evolution feature of Delta Lake

// COMMAND ----------

// DBTITLE 1,Create distinct folders for raw data, one per schemas
dbutils.fs.mkdirs(SCHEMA_V1_RAW_PATH)
dbutils.fs.mkdirs(SCHEMA_V2_RAW_PATH)

// COMMAND ----------

// DBTITLE 1,Download raw files (original schema)
// MAGIC %sh
// MAGIC 
// MAGIC # read path from Cmd 4
// MAGIC path=`cat schema_v1_path.txt`
// MAGIC 
// MAGIC cat <<EOT >> green_tripdata_v1.txt
// MAGIC green_tripdata_2013-08.csv
// MAGIC green_tripdata_2013-09.csv
// MAGIC green_tripdata_2013-10.csv
// MAGIC green_tripdata_2013-11.csv
// MAGIC green_tripdata_2013-12.csv
// MAGIC green_tripdata_2014-01.csv
// MAGIC green_tripdata_2014-02.csv
// MAGIC green_tripdata_2014-03.csv
// MAGIC green_tripdata_2014-04.csv
// MAGIC green_tripdata_2014-05.csv
// MAGIC green_tripdata_2014-06.csv
// MAGIC green_tripdata_2014-07.csv
// MAGIC green_tripdata_2014-08.csv
// MAGIC green_tripdata_2014-09.csv
// MAGIC green_tripdata_2014-10.csv
// MAGIC green_tripdata_2014-11.csv
// MAGIC green_tripdata_2014-12.csv
// MAGIC EOT
// MAGIC 
// MAGIC while read file; do
// MAGIC   wget -P $path http://s3.amazonaws.com/nyc-tlc/trip%20data/$file
// MAGIC done <green_tripdata_v1.txt

// COMMAND ----------

// DBTITLE 1,List files (original schema)
display(dbutils.fs.ls(SCHEMA_V1_RAW_PATH))

// COMMAND ----------

// DBTITLE 1,Download raw files (modified schema)
// MAGIC %sh
// MAGIC 
// MAGIC # read path from Cmd 4
// MAGIC path=`cat schema_v2_path.txt`
// MAGIC 
// MAGIC cat <<EOT >> green_tripdata_v2.txt
// MAGIC green_tripdata_2015-01.csv
// MAGIC green_tripdata_2015-02.csv
// MAGIC green_tripdata_2015-03.csv
// MAGIC green_tripdata_2015-04.csv
// MAGIC green_tripdata_2015-05.csv
// MAGIC green_tripdata_2015-06.csv
// MAGIC green_tripdata_2015-07.csv
// MAGIC green_tripdata_2015-08.csv
// MAGIC green_tripdata_2015-09.csv
// MAGIC green_tripdata_2015-10.csv
// MAGIC green_tripdata_2015-11.csv
// MAGIC green_tripdata_2015-12.csv
// MAGIC green_tripdata_2016-01.csv
// MAGIC green_tripdata_2016-02.csv
// MAGIC green_tripdata_2016-03.csv
// MAGIC green_tripdata_2016-04.csv
// MAGIC green_tripdata_2016-05.csv
// MAGIC green_tripdata_2016-06.csv
// MAGIC EOT
// MAGIC 
// MAGIC while read file; do
// MAGIC   wget -P $path http://s3.amazonaws.com/nyc-tlc/trip%20data/$file
// MAGIC done <green_tripdata_v2.txt

// COMMAND ----------

// DBTITLE 1,List files (modified schema)
display(dbutils.fs.ls(SCHEMA_V2_RAW_PATH))

// COMMAND ----------

// DBTITLE 1,Explicitly define schema (original)
import org.apache.spark.sql.types._

val schema_v1 = new StructType()
  .add("VendorID",IntegerType,true)
  .add("lpep_pickup_datetime",TimestampType,true)
  .add("Lpep_dropoff_datetime",TimestampType,true)
  .add("Store_and_fwd_flag",StringType,true)
  .add("RateCodeID",IntegerType,true)
  .add("Pickup_longitude",DoubleType,true)
  .add("Pickup_latitude",DoubleType,true)
  .add("Dropoff_longitude",DoubleType,true)
  .add("Dropoff_latitude",DoubleType,true)
  .add("Passenger_count",IntegerType,true)
  .add("Trip_distance",DoubleType,true)
  .add("Fare_amount",DoubleType,true)
  .add("Extra",DoubleType,true)
  .add("MTA_tax",DoubleType,true)
  .add("Tip_amount",DoubleType,true)
  .add("Tolls_amount",DoubleType,true)
  .add("Ehail_fee",StringType,true)
  .add("Total_amount",DoubleType,true)
  .add("Payment_type",IntegerType,true)
  .add("Trip_type",IntegerType,true)

// COMMAND ----------

// DBTITLE 1,Read data into DataFrame (original schema)
val green_tripdata_v1 = spark.read
  .format("csv")
  .option("header", "true")
  .schema(schema_v1)
  .load("dbfs:" + SCHEMA_V1_RAW_PATH + "/*.csv")

display(green_tripdata_v1)

// COMMAND ----------

// DBTITLE 1,Write Green Trip Data to Delta
green_tripdata_v1.write
  .format("delta")
  .mode("overwrite")
  .save(BRONZE_PATH)

// COMMAND ----------

// DBTITLE 1,Create Database
// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS cchalc_nyctlc;
// MAGIC USE cchalc_nyctlc;

// COMMAND ----------

// DBTITLE 1,Register table with Metastore
spark.sql("CREATE TABLE green_tripdata_bronze USING DELTA LOCATION '" + BRONZE_PATH + "'")

// COMMAND ----------

// MAGIC %md
// MAGIC # Schema Evolution

// COMMAND ----------

// DBTITLE 1,Explicitly define schema (modified)
val schema_v2 = new StructType()
  .add("VendorID",IntegerType,true)
  .add("lpep_pickup_datetime",TimestampType,true)
  .add("Lpep_dropoff_datetime",TimestampType,true)
  .add("Store_and_fwd_flag",StringType,true)
  .add("RateCodeID",IntegerType,true)
  .add("Pickup_longitude",DoubleType,true)
  .add("Pickup_latitude",DoubleType,true)
  .add("Dropoff_longitude",DoubleType,true)
  .add("Dropoff_latitude",DoubleType,true)
  .add("Passenger_count",IntegerType,true)
  .add("Trip_distance",DoubleType,true)
  .add("Fare_amount",DoubleType,true)
  .add("Extra",DoubleType,true)
  .add("MTA_tax",DoubleType,true)
  .add("Tip_amount",DoubleType,true)
  .add("Tolls_amount",DoubleType,true)
  .add("Ehail_fee",StringType,true)
  .add("improvement_surcharge",DoubleType,true) // this is a new column!
  .add("Total_amount",DoubleType,true)
  .add("Payment_type",IntegerType,true)
  .add("Trip_type",IntegerType,true)

// COMMAND ----------

// DBTITLE 1,Read data into DataFrame (modified schema)
val green_tripdata_v2 = spark.read
  .format("csv")
  .option("header", "true")
  .schema(schema_v2)
  .load("dbfs:" + SCHEMA_V2_RAW_PATH + "/*.csv")

display(green_tripdata_v2)

// COMMAND ----------

// DBTITLE 1,Append to the existing Delta table, automatically adding the new column
green_tripdata_v2.write
  .format("delta")
  .mode("append")
  .option("mergeSchema", "true") // automatically add the new column
  .save(BRONZE_PATH)

// COMMAND ----------

// DBTITLE 1,Use Hilbert Space Filling Curve
spark.conf.set("spark.databricks.io.skipping.mdc.curve", "hilbert")

// COMMAND ----------

// DBTITLE 1,Optimize table for performance
// MAGIC %sql
// MAGIC OPTIMIZE cchalc_nyctlc.green_tripdata_bronze ZORDER BY Pickup_latitude, Pickup_longitude

// COMMAND ----------

// DBTITLE 1,Note that we now have ~45M trip records in our table
// MAGIC %sql
// MAGIC SELECT count(*) FROM cchalc_nyctlc.green_tripdata_bronze

// COMMAND ----------

// DBTITLE 1,We can see that ~17M records have a null value for the new column ...
// MAGIC %sql
// MAGIC SELECT count(*) FROM nyctlc.green_tripdata_bronze WHERE improvement_surcharge IS null

// COMMAND ----------

// DBTITLE 1,... and ~28M records do have a value for the new column
// MAGIC %sql
// MAGIC SELECT count(*) FROM cchalc_nyctlc.green_tripdata_bronze WHERE improvement_surcharge IS NOT null
