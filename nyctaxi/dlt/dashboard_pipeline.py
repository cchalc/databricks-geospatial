# Databricks notebook source
# 
# 2015 NYC Taxi Pipeline 
#

# Use the following command to get the full filepath
#dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get() 

#
# Import 
#
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

#
# Specify schemas
#
schema_taxi_rate_code = StructType([
    StructField("rate_code_id", IntegerType()),
    StructField("rate_code_desc", StringType())
])

schema_taxi_payment_type = StructType([
    StructField("payment_type", IntegerType()),
    StructField("payment_desc", StringType())
])


#
# Lookup Tables
#

# Taxi Rate Code
@dlt.create_table(name="map_rate_code")
def map_rate_code():
  return (
    spark.read.format("csv")
      .schema(schema_taxi_rate_code)
      .option("delimiter", ",")
      .option("header", "true")
      .load("/databricks-datasets/nyctaxi/taxizone/taxi_rate_code.csv")    
  )

@dlt.create_table(name="map_payment_type")
def map_payment_type():
  return (
      spark.read.format("csv")
        .schema(schema_taxi_payment_type)
        .option("delimiter", ",")
        .option("header", "true")
        .load("/databricks-datasets/nyctaxi/taxizone/taxi_payment_type.csv")  
  )
  
@dlt.create_table(name="map_point_to_location")
def map_point_to_location():
  return (
    spark.read.format("delta").load("/user/christopher.chalcraft@databricks.com/nyctaxi/map_point2Location") 
  )

#
# Source View
#

# Green Cab Source View
@dlt.create_table(
   name="src_green_cab"  
)
@dlt.expect("valid pickup_datetime", "lpep_pickup_datetime IS NOT NULL")
@dlt.expect("valid dropoff_datetime", "lpep_dropoff_datetime IS NOT NULL")
def src_green_cab():
  return (
    spark.read.format("delta").load("/user/christopher.chalcraft@databricks.com/nyctaxi/nyctaxi_greencab_source")
  )

# 
# Bronze (bz) Tables 
#
@dlt.table(
    name="bz_green_cab",
    partition_cols=["do_date"],
    table_properties={
        "pipelines.autoOptimize.zOrderCols": "do_datetime", 
        "pipelines.metastore.tableName": "cchalc_nyctaxi.bz_green_cab"
    }
)
@dlt.expect_or_drop("valid do_date", "do_date IS NOT NULL")
def bz_green_cab():
  return (
    dlt.read("src_green_cab")
        .withColumnRenamed("lpep_dropoff_datetime", "do_datetime")
        .withColumnRenamed("lpep_pickup_datetime", "pu_datetime")
        .withColumnRenamed("dropoff_latitude", "do_lat")
        .withColumnRenamed("dropoff_longitude", "do_long")
        .withColumnRenamed("RatecodeID", "rate_code_id")
        .withColumn("do_date", expr("DATE(do_datetime)"))
        .withColumn("hour", expr("HOUR(do_datetime) AS hour"))
  )
  
  
# 
# Silver (Ag) Tables
# 

@dlt.table(
    name="ag_green_cab",
    partition_cols=["do_date"],
    table_properties={
        "pipelines.autoOptimize.zOrderCols": "do_datetime", 
    }
)
#@expect("non zero passenger count", "passenger_count > 0")
#@expect_or_fail("non zero passenger count", "passenger_count > 0")
@dlt.expect_or_drop("non zero passenger count", "passenger_count > 0")
def ag_green_cab():
  ft = dlt.read("bz_green_cab")
  rc = dlt.read("map_rate_code")
  pt = dlt.read("map_payment_type")
  pl = dlt.read("map_point_to_location")
  return (
      ft.join(rc, rc.rate_code_id == ft.rate_code_id)
        .join(pt, pt.payment_type == ft.payment_type)
        .join(pl, (pl.dropoff_latitude == ft.do_lat) & (pl.dropoff_longitude == ft.do_long) & (pl.lpep_dropoff_datetime == ft.do_datetime))
        .select("do_datetime", "pu_datetime", "do_date", "hour", "passenger_count", "do_lat", "do_long", 
                "rate_code_desc", "payment_desc", "borough", "zone", 
                "fare_amount", "extra", "tip_amount", "tolls_amount", "total_amount")  
  )  
  
# 
# Gold Tables (Au)
# 
  
# Summary Stats  
@dlt.table(
  name="au_summary_stats",
  table_properties={
        "pipelines.metastore.tableName": "cchalc_nyctaxi.au_summary_stats"
    } 
)  
def au_summary_stats():
  return(
      dlt.read("bz_green_cab")
         .groupBy("do_date").agg(
            expr("COUNT(DISTINCT pu_datetime) AS pickups"), 
            expr("COUNT(DISTINCT do_datetime) AS dropoffs"),
            expr("COUNT(1) AS trips")
          )
  )

# Payment Type By Hour (across time)
@dlt.create_table(
    name="au_payment_by_hour",
    table_properties={
        "pipelines.metastore.tableName": "cchalc_nyctaxi.au_payment_by_hour"
    } 
)
def au_payment_by_hour():
    return (
        dlt.read("ag_green_cab")
           .groupBy("hour", "payment_desc").agg(expr("SUM(total_amount) AS total_amount"))


    )
  
  
# Four (of the 5) main boroughs
@dlt.create_table(
    name="au_boroughs",
    table_properties={
        "pipelines.metastore.tableName": "cchalc_nyctaxi.au_boroughs"
    } 
  
)
@dlt.expect_or_drop("total fare amount is > $3.00", "total_amount > 3.00")
def au_boroughs():
    return (
        dlt.read("ag_green_cab")
           .where(expr("borough IN ('Bronx', 'Brooklyn', 'Queens', 'Manhattan')"))
           .select("borough", "do_datetime", "do_lat", "do_long", "rate_code_desc", "payment_desc", "zone", "total_amount")
    )
  

# # Expectation Log table
# @dlt.create_table(
#     name="expectation_log",
#     table_properties={
#         "pipelines.autoOptimize.zOrderCols": "do_datetime", 
#         "pipelines.metastore.tableName": "cchalc_nyctaxi.expectations_log"
#     }  
# )
# def expectation_log():
#   pipelines_id = spark.conf.get("pipelines.id")
#   sqlQuery = """SELECT id, origin, timestamp, details 
#                   FROM delta.`dbfs:/pipelines/""" + pipelines_id + """/system/events/` 
#                  WHERE details LIKE '%flow_progress%data_quality%expectations%'"""
#   df = spark.sql(sqlQuery)
#   schema = schema_of_json("""{"flow_progress":{
#                                 "status":"COMPLETED",
#                                 "metrics":{"num_output_rows":91939},
#                                 "data_quality":{"dropped_records":32,
#                                 "expectations":[{"name":"non zero passenger count","dataset":"silver_GreenCab","passed_records":91939,"failed_records":32}]}}
#                               }""")      
#   df_expectations = df.withColumn("details_json", from_json(df.details, schema))
#   return (
#       df_expectations.select("id", "timestamp", "origin.pipeline_id", "origin.pipeline_name", "origin.cluster_id", "origin.flow_id", "origin.flow_name", "details_json") 
#   )

#   createTable("expectations_log")
#     .query {
#         val pipelinesId = spark.conf.get("pipelines.id")
#         //val pipelinesId = "59cf8076-61aa-488b-9139-edba476b0c91"
#         val sqlQuery = """SELECT id, origin, timestamp, details 
#                             FROM delta.`dbfs:/pipelines/""" + pipelinesId + """/system/events/` 
#                            WHERE details LIKE '%flow_progress%data_quality%expectations%'"""
#         val df = spark.sql(sqlQuery)
#         val schema = schema_of_json("""{"flow_progress":{
#                                       "status":"COMPLETED",
#                                       "metrics":{"num_output_rows":91939},
#                                       "data_quality":{"dropped_records":32,
#                                       "expectations":[{"name":"non zero passenger count","dataset":"silver_GreenCab","passed_records":91939,"failed_records":32}]}}
#                                      }""")      
#         //val schema = schema_of_json(lit(df.select($"details").as[String].first))
#         val df_expectations = df.withColumn("details_json", from_json($"details", schema, Map[String, String]().asJava))
#         df_expectations.select("id", "timestamp", "origin.pipeline_id", "origin.pipeline_name", "origin.cluster_id", "origin.flow_id", "origin.flow_name", "details_json") 
#     }
#    .tableProperty("pipelines.autoOptimize.zOrderCols", "do_datetime")
#    .tableProperty("pipelines.metastore.tableName", "DAIS21.expectations_log")


