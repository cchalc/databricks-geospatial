# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC # Geospatial Processing on Databricks
# MAGIC 
# MAGIC **Author:** [Derek Yeager](https://www.linkedin.com/in/derekcyeager/) (derek@databricks.com)
# MAGIC 
# MAGIC <img src="https://1fykyq3mdn5r21tpna3wkdyi-wpengine.netdna-ssl.com/wp-content/uploads/2018/06/image12.png" alt="drawing" width="200"/>
# MAGIC 
# MAGIC All of the Notebooks included in this series are intended to be reproducible using the [nyctaxi](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) dataset.  Specifically, we focus on the Green Taxi Trip Records (~45M).  To test at larger scale, consider including the Yellow Taxi Trip Records as well as the For-Hire Vehicle Trip Records (Uber, Lyft).  See below for more information on the NYC Taxi & Limousine Commision data.
# MAGIC 
# MAGIC **Notebook Examples:**
# MAGIC 
# MAGIC 1. <a href="$./nyctaxi%20-%2001%20download%20and%20ingest%20dataset">Download and ingest dataset (green trip records only)</a>
# MAGIC 2. (optional) <a href="$./nyctaxi%20-%2002%20enriched%20Delta%20table%20(H3)">create Delta Lake Silver Table</a>
# MAGIC 3. <a href="$./nyctaxi%20-%20Distributed%20Clustering%20(geoscan)">Distributed Clustering (Geoscan)</a>
# MAGIC 4. <a href="$./nyctaxi%20-%20Spatial%20Disaggregation%20(H3,%20Kepler.gl)">Spatial Disaggregation (Kepler.gl + H3)</a>
# MAGIC 5. Point-in-polygon join:
# MAGIC   - (prerequisite) <a href="$./sedona%2001%20load%20polygons">Load polygons (geojson example)</a>
# MAGIC   - <a href="$./sedona%2002%20point-in-polygon%20SQL">Apache Sedona SQL API implementation</a>
# MAGIC   - <a href="$./sedona%2003%20point-in-polygon%20RDD">Apache Sedona RDD API implementation</a>
# MAGIC   - H3 implementation (**coming soon**)
# MAGIC   
# MAGIC **See also - Solution Accelerators**
# MAGIC - [Identifying Financial Fraud With Geospatial Clustering](https://databricks.com/blog/2021/04/13/identifying-financial-fraud-with-geospatial-clustering.html)
# MAGIC - [Leveraging ESG Data to Operationalize Sustainability](https://databricks.com/blog/2020/11/11/leveraging-esg-data-to-operationalize-sustainability.html)

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="/files/derek.yeager@databricks.com/georefarch.png" width="80%">

# COMMAND ----------


