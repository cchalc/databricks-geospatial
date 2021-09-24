# Databricks notebook source
# 
# 2015 NYC Taxi Pipeline 
#

# Use the following command to get the full filepath
#dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get() 

#
# Import 
#
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
@create_view(name="map_rate_code")
def map_rate_code():
  return (
    spark.read.format("csv")
      .schema(schema_taxi_rate_code)
      .option("delimiter", ",")
      .option("header", "true")
      .load("/databricks-datasets/nyctaxi/taxizone/taxi_rate_code.csv")    
  )

@create_view(name="map_payment_type")
def map_payment_type():
  return (
      spark.read.format("csv")
        .schema(schema_taxi_payment_type)
        .option("delimiter", ",")
        .option("header", "true")
        .load("/databricks-datasets/nyctaxi/taxizone/taxi_payment_type.csv")  
  )
  
@create_view(name="map_point_to_location")
def map_point_to_location():
  return (
    spark.read.format("delta").load("/user/denny.lee/nyctaxi/map_point2Location") 
  )

#
# Source View
#
