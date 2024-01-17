# Databricks notebook source
# MAGIC %md Check script

# COMMAND ----------

# MAGIC %md make sure that databricks-mosaic is installed on cluster

# COMMAND ----------

# MAGIC %fs head dbfs:/FileStore/geospatial/mosaic/gdal/mosaic-gdal-init.sh

# COMMAND ----------

file_path = "/dbfs/FileStore/geospatial/mosaic/gdal/mosaic-gdal-init.sh"

# COMMAND ----------


# Read the file contents
with open(file_path, 'r') as file:
    file_contents = file.read()

# Display the contents
print(file_contents)

# COMMAND ----------

# MAGIC %r
# MAGIC R.version

# COMMAND ----------

# MAGIC %sh gdalinfo --version

# COMMAND ----------

# MAGIC %r
# MAGIC install.packages(c('rgdal'),repos = "http://cran.case.edu", configure.args=c("--with-proj-include=/packages/PROJ/6.1.0/include","--with-proj-lib=/packages/PROJ/6.1.0/lib"))

# COMMAND ----------

# MAGIC %md Load a file

# COMMAND ----------

# MAGIC %r
# MAGIC library(rgdal)

# COMMAND ----------

# MAGIC %md #### Some notes on GDAL
# MAGIC - [Why have CRS, projections and transformations changed?
# MAGIC ](https://rgdal.r-forge.r-project.org/articles/CRS_projections_transformations.html)
# MAGIC - [R spatial follows GDAL and PROJ development](https://r-spatial.org/r/2020/03/17/wkt.html)
# MAGIC

# COMMAND ----------

# MAGIC %r
# MAGIC ogrDrivers()$name

# COMMAND ----------

# MAGIC %md # NYC Taxi

# COMMAND ----------

# MAGIC %python
# MAGIC user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
# MAGIC
# MAGIC raw_path = f"dbfs:/tmp/mosaic/{user_name}"
# MAGIC raw_taxi_zones_path = f"{raw_path}/taxi_zones"
# MAGIC
# MAGIC print(f"The raw data will be stored in {raw_path}")

# COMMAND ----------

# MAGIC %python
# MAGIC import requests
# MAGIC import pathlib
# MAGIC
# MAGIC taxi_zones_url = 'https://data.cityofnewyork.us/api/geospatial/d3c5-ddgc?method=export&format=GeoJSON'
# MAGIC
# MAGIC # The DBFS file system is mounted under /dbfs/ directory on Databricks cluster nodes
# MAGIC local_taxi_zones_path = pathlib.Path(raw_taxi_zones_path.replace('dbfs:/', '/dbfs/'))
# MAGIC local_taxi_zones_path.mkdir(parents=True, exist_ok=True)
# MAGIC
# MAGIC req = requests.get(taxi_zones_url)
# MAGIC with open(local_taxi_zones_path / f'nyc_taxi_zones.geojson', 'wb') as f:
# MAGIC   f.write(req.content)
# MAGIC   
# MAGIC display(dbutils.fs.ls(raw_taxi_zones_path))

# COMMAND ----------

# MAGIC %fs ls /tmp/mosaic/christopher.chalcraft@databricks.com/taxi_zones/nyc_taxi_zones.geojson

# COMMAND ----------

# MAGIC %r
# MAGIC taxi_zones <- readOGR(dsn="/dbfs/tmp/mosaic/christopher.chalcraft@databricks.com/taxi_zones/nyc_taxi_zones.geojson", layer="nyc_taxi_zones")

# COMMAND ----------

# MAGIC %r
# MAGIC # terra::vect or sf::st_read.

# COMMAND ----------

# MAGIC %md # Terra / SF

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %r
# MAGIC -- 

# COMMAND ----------

# MAGIC %md # AIS Data

# COMMAND ----------

# MAGIC %r
# MAGIC dbutils.fs.mkdirs("/tmp/ship2ship")

# COMMAND ----------

# MAGIC %sh
# MAGIC # see: https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2018/index.html
# MAGIC # we download data to dbfs:// mountpoint (/dbfs)
# MAGIC mkdir /ship2ship/
# MAGIC cd /ship2ship/
# MAGIC wget -np -r -nH -L --cut-dirs=4 https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2018/AIS_2018_01_31.zip > /dev/null 2>&1
# MAGIC unzip AIS_2018_01_31.zip
# MAGIC mv AIS_2018_01_31.csv /dbfs/tmp/ship2ship/

# COMMAND ----------

# MAGIC %r
# MAGIC ais_data <- readOGR(dsn="/tmp/ship2ship", layer="AIS_2018_01_31")

# COMMAND ----------


