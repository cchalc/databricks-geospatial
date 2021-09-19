// Databricks notebook source
// MAGIC %md
// MAGIC # Geospatial Processing on Databricks
// MAGIC 
// MAGIC **Author:** [Derek Yeager](https://www.linkedin.com/in/derekcyeager/) (derek@databricks.com)
// MAGIC 
// MAGIC <img src="https://1fykyq3mdn5r21tpna3wkdyi-wpengine.netdna-ssl.com/wp-content/uploads/2018/06/image12.png" alt="drawing" width="200"/>
// MAGIC 
// MAGIC Create Delta Lake enriched table (Silver) with precomputed H3 hex values at multiple resolutions.

// COMMAND ----------

// MAGIC %md # Compute H3 index at multiple resolutions

// COMMAND ----------

// MAGIC %sql
// MAGIC USE nyctlc

// COMMAND ----------

// MAGIC %md init script located at `
// MAGIC dbfs:/derek/init/init.sh`

// COMMAND ----------

// MAGIC %md link `com.uber:h3:3.7.0` from Maven Central

// COMMAND ----------

// DBTITLE 1,imports
import com.uber.h3core.H3Core

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

// COMMAND ----------

// DBTITLE 1,UDF converts { lat, long } to H3 index
@transient lazy val h3 = new ThreadLocal[H3Core] {
  override def initialValue() = H3Core.newInstance()
}

def convertToH3Address(xLong: Double, yLat: Double, precision: Int): String = {
  h3.get.geoToH3Address(yLat, xLong, precision)
}

// Register UDF
val geoToH3Address: UserDefinedFunction = udf(convertToH3Address _)

// COMMAND ----------

val green_bronze = spark.table("green_tripdata_bronze")
display(green_bronze)

// COMMAND ----------

// DBTITLE 1,add H3 index for pickup location
val resolutions = Array.range(5, 10) // index at multiple H3 resolutions

// column names
val latCol = "pickup_latitude"
val lngCol = "pickup_longitude"
val colPrefix = "pickup_h3_res_"

// index at various resolutions
val green_h3 = resolutions.foldLeft(green_bronze)( 
  (df, res) => df.withColumn(colPrefix + res, geoToH3Address(col(lngCol), col(latCol), lit(res)))
)
//.repartition(sc.defaultParallelism * 10) // smooth out any skew
.cache() // cache on the cluster

display(green_h3)

// COMMAND ----------

// MAGIC %md # Examine

// COMMAND ----------

// DBTITLE 1,create temp view
green_h3.createOrReplaceTempView("green_h3")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM green_h3 LIMIT 5

// COMMAND ----------

// DBTITLE 1,examine distribution
// MAGIC %sql
// MAGIC SELECT 
// MAGIC   COUNT(DISTINCT pickup_h3_res_5 ) AS res5_count,
// MAGIC   COUNT(DISTINCT pickup_h3_res_6 ) AS res6_count,
// MAGIC   COUNT(DISTINCT pickup_h3_res_7 ) AS res7_count,
// MAGIC   COUNT(DISTINCT pickup_h3_res_8 ) AS res8_count,
// MAGIC   COUNT(DISTINCT pickup_h3_res_9 ) AS res9_count
// MAGIC FROM green_h3;

// COMMAND ----------

// DBTITLE 1,resolution 7 distribution
// MAGIC %sql 
// MAGIC SELECT 
// MAGIC   pickup_h3_res_7, 
// MAGIC   count(pickup_h3_res_7) AS count 
// MAGIC FROM 
// MAGIC   green_h3 
// MAGIC GROUP BY 
// MAGIC   pickup_h3_res_7 
// MAGIC ORDER BY 
// MAGIC   count DESC

// COMMAND ----------

// MAGIC %md # Write to Delta

// COMMAND ----------

/* Uncomment to persist H3-enriched table to Delta
green_h3
  .write
  .format("delta")
  .saveAsTable("green_tripdata_silver")
*/
