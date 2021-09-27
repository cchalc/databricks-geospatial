# Databricks notebook source
# MAGIC %md # Spatial Disaggregation with H3 and Kepler.gl
# MAGIC 
# MAGIC **Author:** [Derek Yeager](https://www.linkedin.com/in/derekcyeager/) (derek@databricks.com)
# MAGIC 
# MAGIC <img src="https://1fykyq3mdn5r21tpna3wkdyi-wpengine.netdna-ssl.com/wp-content/uploads/2018/06/image12.png" alt="drawing" width="200"/>
# MAGIC 
# MAGIC This Notebook presents a technique to "rasterize" a large point dataset for visualization in Kepler.gl **(eye candy in final cell)**.  This is a useful technique to visualize large spatial datasets with high fidelity.  We use an aggregation technique using [Uber’s Hexagonal Hierarchical Spatial Index (H3)](https://eng.uber.com/h3/).  In this example, we visualize 45M records from the [nyctaxi](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) dataset. 

# COMMAND ----------

# MAGIC %md # Initial setup

# COMMAND ----------

# MAGIC %pip install keplergl

# COMMAND ----------

from h3 import h3
import pandas as pd
import pyspark.sql.functions as sf
from pyspark.sql.types import StructType, StructField, DoubleType

# COMMAND ----------

# MAGIC %md allow Kepler.gl to embed cleanly in Databricks

# COMMAND ----------

import keplergl
import tempfile
 
orig_repr_html_ = keplergl.KeplerGl._repr_html_
def html_patch(self):
  """This is the patch to make it work well in Databricks"""
  # temp file purposely doesn't get cleaned up, but should if that's desired
  (_, tmp) = tempfile.mkstemp() 
  frame_bytes = orig_repr_html_(self)
  frame_str = str(frame_bytes, encoding='utf-8')
  # This additional script for fixing the height is necessary because kepler doesn't embed well, and
  # mutates containing document directly. The height parameter to keplergl.KeplerGl will be the initial 
  # height of the result iframe. 
  return f"""{frame_str}<script>
    var targetHeight = "{self.height or 600}px";
    var interval = window.setInterval(function() {{
      if (document.body && document.body.style && document.body.style.height !== targetHeight) {{
        document.body.style.height = targetHeight;
      }}
    }}, 250);</script>""";
setattr(keplergl.KeplerGl, '_repr_html_', html_patch)

# COMMAND ----------

# MAGIC %md define H3 UDFs

# COMMAND ----------

@udf
def h3_hex_udf(lat, lon, resolution):
  """
   UDF to get the H3 value based on lat/lon at a given resolution
  """
  return h3.geo_to_h3(lat, lon, resolution)

schema = StructType([
  StructField("lat", DoubleType(), False),
  StructField("lon", DoubleType(), False)
])

@udf(schema)
def h3_hex_centroid(hex):
  """
  Get the centroid from a h3 hex location
  """
  (lat, lon) = h3.h3_to_geo(hex)
  return {
    "lat": lat,
    "lon": lon
  }

# COMMAND ----------

# MAGIC %md # Compute Aggregation

# COMMAND ----------

# set this 0-15 (see: https://h3geo.org/docs/core-library/restable/)
h3_resolution = 11

# COMMAND ----------

# MAGIC %md 
# MAGIC - read our point data, computing the H3 index value for each pickup
# MAGIC - group by the H3 values and compute counts
# MAGIC - add the centroid for each hex
# MAGIC - rename columns and filter

# COMMAND ----------

h3_aggregation = (
  spark.read.table("cchalc_nyctlc.green_tripdata_bronze")
    
    # compute h3 grid location at a given resolution for each point
    .withColumn(
      "h3", 
      h3_hex_udf(sf.col("Pickup_latitude"), sf.col("Pickup_longitude"), sf.lit(h3_resolution)))
  
    # group by the h3 grid
    .groupBy("h3")
  
    # grab counts
    .count()
  
    # add the centroid  
    .withColumn("h3_centroid", h3_hex_centroid(sf.col("h3")))
  
    # rename columns  
    .select(
      "h3",
      "count",
      sf.col("h3_centroid.lat").alias("lat"),
      sf.col("h3_centroid.lon").alias("lon"),
      sf.log10("count").alias("log_count")
    )
  
    # ensure sparse representation
    .filter(sf.col("count") > sf.lit(0))
)

# COMMAND ----------

# MAGIC %md convert results to Pandas

# COMMAND ----------

pandas_df = h3_aggregation.toPandas()

# COMMAND ----------

# MAGIC %md export results to CSV

# COMMAND ----------

# MAGIC %fs ls dbfs:/home/christopher.chalcraft@databricks.com/nyctaxi/

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/home/christopher.chalcraft@databricks.com/nyctaxi/green_rollups")

# COMMAND ----------

pandas_df.head()

# COMMAND ----------

pandas_df.to_csv(f"/dbfs/home/christopher.chalcraft@databricks.com/nyctaxi/green_rollups/out_{h3_resolution}_.csv", index=False, header=True)

# COMMAND ----------

# MAGIC %md # Visualize results with Kepler.gl

# COMMAND ----------

# MAGIC %md Kepler.gl map configuration

# COMMAND ----------

config = {
  "version": "v1",
  "config": {
    "visState": {
      "filters": [],
      "layers": [
        {
          "id": "ghqc9i",
          "type": "point",
          "config": {
            "dataId": "points",
            "label": "Point",
            "color": [
              18,
              147,
              154
            ],
            "columns": {
              "lat": "lat",
              "lng": "lon",
              "altitude": None
            },
            "isVisible": True,
            "visConfig": {
              "radius": 3,
              "fixedRadius": False,
              "opacity": 0.79,
              "outline": False,
              "thickness": 2,
              "strokeColor": None,
              "colorRange": {
                "name": "Global Warming",
                "type": "sequential",
                "category": "Uber",
                "colors": [
                  "#5A1846",
                  "#900C3F",
                  "#C70039",
                  "#E3611C",
                  "#F1920E",
                  "#FFC300"
                ]
              },
              "strokeColorRange": {
                "name": "Global Warming",
                "type": "sequential",
                "category": "Uber",
                "colors": [
                  "#5A1846",
                  "#900C3F",
                  "#C70039",
                  "#E3611C",
                  "#F1920E",
                  "#FFC300"
                ]
              },
              "radiusRange": [
                0,
                50
              ],
              "filled": True
            },
            "hidden": False,
            "textLabel": [
              {
                "field": None,
                "color": [
                  255,
                  255,
                  255
                ],
                "size": 18,
                "offset": [
                  0,
                  0
                ],
                "anchor": "start",
                "alignment": "center"
              }
            ]
          },
          "visualChannels": {
            "colorField": {
              "name": "log_count",
              "type": "real"
            },
            "colorScale": "quantile",
            "strokeColorField": None,
            "strokeColorScale": "quantile",
            "sizeField": None,
            "sizeScale": "linear"
          }
        },
        {
          "id": "t4cmhph",
          "type": "hexagonId",
          "config": {
            "dataId": "points",
            "label": "h3",
            "color": [
              221,
              178,
              124
            ],
            "columns": {
              "hex_id": "h3"
            },
            "isVisible": False,
            "visConfig": {
              "opacity": 0.8,
              "colorRange": {
                "name": "Global Warming",
                "type": "sequential",
                "category": "Uber",
                "colors": [
                  "#5A1846",
                  "#900C3F",
                  "#C70039",
                  "#E3611C",
                  "#F1920E",
                  "#FFC300"
                ]
              },
              "coverage": 1,
              "enable3d": False,
              "sizeRange": [
                0,
                500
              ],
              "coverageRange": [
                0,
                1
              ],
              "elevationScale": 5
            },
            "hidden": False,
            "textLabel": [
              {
                "field": None,
                "color": [
                  255,
                  255,
                  255
                ],
                "size": 18,
                "offset": [
                  0,
                  0
                ],
                "anchor": "start",
                "alignment": "center"
              }
            ]
          },
          "visualChannels": {
            "colorField": None,
            "colorScale": "quantile",
            "sizeField": None,
            "sizeScale": "linear",
            "coverageField": None,
            "coverageScale": "linear"
          }
        }
      ],
      "interactionConfig": {
        "tooltip": {
          "fieldsToShow": {
            "points": [
              {
                "name": "h3",
                "format": None
              },
              {
                "name": "count",
                "format": None
              },
              {
                "name": "log_count",
                "format": None
              }
            ]
          },
          "compareMode": False,
          "compareType": "absolute",
          "enabled": True
        },
        "brush": {
          "size": 0.5,
          "enabled": False
        },
        "geocoder": {
          "enabled": False
        },
        "coordinate": {
          "enabled": False
        }
      },
      "layerBlending": "normal",
      "splitMaps": [],
      "animationConfig": {
        "currentTime": None,
        "speed": 1
      }
    },
    "mapState": {
      "bearing": 0,
      "dragRotate": False,
      "latitude": 40.68891347055761,
      "longitude": -73.96521567458855,
      "pitch": 0,
      "zoom": 10.330647340191392,
      "isSplit": False
    },
    "mapStyle": {
      "styleType": "dark",
      "topLayerGroups": {},
      "visibleLayerGroups": {
        "label": True,
        "road": True,
        "border": False,
        "building": True,
        "water": True,
        "land": True,
        "3d building": False
      },
      "threeDBuildingColor": [
        9.665468314072013,
        17.18305478057247,
        31.1442867897876
      ],
      "mapStyles": {}
    }
  }
}

# COMMAND ----------

map_1 = keplergl.KeplerGl(height=600, config=config)
map_1.add_data(data = pandas_df, name = "points")
displayHTML(map_1._repr_html_())
