# Databricks notebook source
# DBTITLE 1,Spatial Indexing
# MAGIC %md
# MAGIC 
# MAGIC Efficient spatial queries at scale requires putting the geometries into a **spatial index**.
# MAGIC 
# MAGIC Querying indexed data with geometric predicate pushdown can dramatically improve performance
# MAGIC 
# MAGIC **R Trees** and **H3** are two common spatial indexing techniques
# MAGIC 
# MAGIC Almost all spatial data structures share the same principle to enable efficient search: branch and bound. It means arranging data in a tree-like structure that allows discarding branches at once if they do not fit our search criteria (data skipping).
# MAGIC <br>
# MAGIC In this notebook we will use H3 together with KeplerGL to demonstrate easy visualisation over large volumes of data and integration between different libraries in python.

# COMMAND ----------

# DBTITLE 1,Space Filling Curves
# MAGIC %md
# MAGIC 
# MAGIC H3 is a spatial index developed by UBER and it is based on hierarchical hexagons that envelop whole surface of the earth. <br>
# MAGIC <img src="https://1fykyq3mdn5r21tpna3wkdyi-wpengine.netdna-ssl.com/wp-content/uploads/2018/06/image12.png" width=300>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Ensure you are using a DBR with Spark 3.0, Java 1.8, Scala 2.12, and Python 3.7+ (tested on 7.3 LTS)
# MAGIC 
# MAGIC Link the following libraries to your cluster:
# MAGIC 
# MAGIC - **PyPi**
# MAGIC     - `h3`
# MAGIC     - `keplergl`
# MAGIC     - `geopandas`
# MAGIC     - `shapely`

# COMMAND ----------

# MAGIC %pip install geopandas h3 shapely keplergl

# COMMAND ----------

import geopandas as gpd
import h3
import shapely
from pyspark.sql.types import *
from pyspark.sql import functions as F
from keplergl import KeplerGl 
import pandas as pd
import numpy

# COMMAND ----------

# DBTITLE 1,Extra steps for KeplerGL rendering
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

# DBTITLE 1,Loading raw neighbourhoods data
# MAGIC %md
# MAGIC Second raw dataset that we will be using is a set of polygons describing NYC neighbourhoods.<br>
# MAGIC This data is stored in geojson format and we will leverage here geopandas library to easily load this data into spark. <br>
# MAGIC Note that this isnt large data so it can be loaded into spark via library such as geopandas that resides on a single node.

# COMMAND ----------

neighborhoods_pd = gpd.read_file("/dbfs/FileStore/milos_colic/neighborhoods.geojson.txt")
display(neighborhoods_pd.head())

# COMMAND ----------

# DBTITLE 1,Generating bronze neighbourhood table
# MAGIC %md
# MAGIC We will generate a silver table in delta based on the geopandas dataframe and some data manipulation in order to make data serializable. <br>
# MAGIC We will encode polygon objects as wkt strings and store this new data as our bronze neighbourhood table. 

# COMMAND ----------

neighborhoods_pd["wkt_polygons"] = neighborhoods_pd["geometry"].apply(lambda g: str(g.to_wkt()))
neighborhoods_df = spark.createDataFrame(neighborhoods_pd.drop("geometry", axis=1))
display(neighborhoods_df)

# COMMAND ----------

#diagonal is a max distance between any pair of 2 points
#hexagon is a symetric geometry, it is enough to pass once through the set of points to get the diagon
#diag/2 corresponds to the radius of minimum bounding circle in this specific case
def hex_max_distance(polygon):
  coords = polygon.boundary.coords.xy
  xs = coords[0]
  ys = coords[1]
  points = [shapely.geometry.Point(xs[i], ys[i]) for i in range(0, len(xs))]
  return max([max([pj.distance(pi) for pi in points if pi != pj]) for pj in points])

# COMMAND ----------

@udf(ArrayType(StructType([StructField("hex", StringType()), StructField("is_dirty", BooleanType())])))
def polygon2H3_optimized_line_buffer(geometry, resolution): 
  def to_h3(geometry):
    if 'MultiPolygon' in geometry.geom_type:
      geometry = list(geometry.geoms)
    else:
      geometry = [geometry]
      
    hex_set = set()
    for p in geometry:
      p_geojson = shapely.geometry.mapping(p)
      hex_set = hex_set.union(h3.polyfill_geojson(p_geojson, resolution))
      
    return hex_set
  
  polygon = shapely.wkt.loads(geometry)
  
  cent = polygon.centroid.xy
  centroid_hex = h3.geo_to_h3(cent[1][0], cent[0][0], resolution)
  hex_polygon = shapely.geometry.Polygon(h3.h3_to_geo_boundary(centroid_hex, geo_json=True))
  buffer_r = hex_max_distance(hex_polygon)
  
  dirty = polygon.boundary.buffer(0.5*buffer_r)
  
  original_set = to_h3(polygon)
  dirty_set = to_h3(dirty)
  
  result = [(hex_i, hex_i in dirty_set) for hex_i in list(original_set.union(dirty_set))]
  
  return result

# COMMAND ----------

hexagon_layer_optimized_res_9 = neighborhoods_df.select(
  F.col("neighborhood").alias("id"), F.col("wkt_polygons")
).withColumn(
  "h3_hexes", polygon2H3_optimized_line_buffer(F.col("wkt_polygons"), F.lit(10))
).select(
  F.explode("h3_hexes").alias("id"),
  F.lit(1).alias("count")
).select(
  F.col("id.hex").alias("id"),
  F.col("id.is_dirty").alias("is_dirty"),
  F.col("count")
)

display(hexagon_layer_optimized_res_9)

# COMMAND ----------

dirty_hexes_layer = hexagon_layer_optimized_res_9.where(F.col("is_dirty")).select("id", "count").toPandas()
clean_hexes_layer = hexagon_layer_optimized_res_9.where(~F.col("is_dirty")).select("id", "count").toPandas()

# COMMAND ----------

plot7_config = {
  "version": "v1",
  "config": {
    "visState": {
      "filters": [],
      "layers": [
        {
          "id": "0rgrrl6",
          "type": "geojson",
          "config": {
            "dataId": "polygon",
            "label": "polygon",
            "color": [
              246,
              218,
              0
            ],
            "columns": {
              "geojson": "wkt_polygons"
            },
            "isVisible": True,
            "visConfig": {
              "opacity": 0.3,
              "strokeOpacity": 0.8,
              "thickness": 0.5,
              "strokeColor": [
                221,
                178,
                124
              ],
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
              "radius": 10,
              "sizeRange": [
                0,
                10
              ],
              "radiusRange": [
                0,
                50
              ],
              "heightRange": [
                0,
                500
              ],
              "elevationScale": 5,
              "stroked": True,
              "filled": True,
              "enable3d": False,
              "wireframe": False
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
            "strokeColorField": None,
            "strokeColorScale": "quantile",
            "heightField": None,
            "heightScale": "linear",
            "radiusField": None,
            "radiusScale": "linear"
          }
        },
        {
          "id": "mj2q8al",
          "type": "geojson",
          "config": {
            "dataId": "hexes_extended_res_10",
            "label": "new layer",
            "color": [
              18,
              147,
              154
            ],
            "columns": {
              "geojson": "wkt_polygons"
            },
            "isVisible": True,
            "visConfig": {
              "opacity": 0.8,
              "strokeOpacity": 0.8,
              "thickness": 0.5,
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
              "radius": 10,
              "sizeRange": [
                0,
                10
              ],
              "radiusRange": [
                0,
                50
              ],
              "heightRange": [
                0,
                500
              ],
              "elevationScale": 5,
              "stroked": True,
              "filled": True,
              "enable3d": False,
              "wireframe": False
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
            "strokeColorField": None,
            "strokeColorScale": "quantile",
            "heightField": None,
            "heightScale": "linear",
            "radiusField": None,
            "radiusScale": "linear"
          }
        },
        {
          "id": "bbylm1o",
          "type": "hexagonId",
          "config": {
            "dataId": "dirty",
            "label": "new layer",
            "color": [
              255,
              153,
              31
            ],
            "columns": {
              "hex_id": "id"
            },
            "isVisible": True,
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
        },
        {
          "id": "g7ha3t8",
          "type": "hexagonId",
          "config": {
            "dataId": "clean",
            "label": "new layer",
            "color": [
              255,
              254,
              230
            ],
            "columns": {
              "hex_id": "id"
            },
            "isVisible": True,
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
            "polygon": [
              {
                "name": "id",
                "format": None
              }
            ],
            "hexes_extended_res_10": [
              {
                "name": "id",
                "format": None
              }
            ],
            "dirty": [
              {
                "name": "id",
                "format": None
              },
              {
                "name": "count",
                "format": None
              }
            ],
            "clean": [
              {
                "name": "id",
                "format": None
              },
              {
                "name": "count",
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
      "latitude": 40.73230206283967,
      "longitude": -73.877371602061,
      "pitch": 0,
      "zoom": 10.488198134727853,
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

m7 = KeplerGl(height=800, config=plot7_config)
m7.add_data(data=dirty_hexes_layer, name = "dirty")
m7.add_data(data=clean_hexes_layer, name = "clean")

displayHTML(m7._repr_html_())

# COMMAND ----------

@udf(ArrayType(StructType([
  StructField("hex", StringType()), 
  StructField("is_dirty", BooleanType()),
  StructField("wkt_chip", StringType())
])))
def polygon2H3_with_chips(geometry, resolution): 
  def to_h3(geometry):
    if 'MultiPolygon' in geometry.geom_type:
      geometry = list(geometry.geoms)
    else:
      geometry = [geometry]
      
    hex_set = set()
    for p in geometry:
      p_geojson = shapely.geometry.mapping(p)
      hex_set = hex_set.union(h3.polyfill_geojson(p_geojson, resolution))
      
    return hex_set
  
  def get_result_struct(hex_i, polygon, dirty_set):
    if hex_i in dirty_set:
      hex_polygon = shapely.geometry.Polygon(h3.h3_to_geo_boundary(hex_i, geo_json=True))
      intersection = polygon.intersection(hex_polygon)
      wkt_result = intersection.to_wkt()
      if "EMPTY" in wkt_result:
        return None
      elif intersection.equals(hex_polygon):
        return (hex_i, False, None)
      else:
        return (hex_i, True, wkt_result)
    else:
      return (hex_i, False, None)
  
  polygon = shapely.wkt.loads(geometry)
  
  cent = polygon.centroid.xy
  centroid_hex = h3.geo_to_h3(cent[1][0], cent[0][0], resolution)
  hex_polygon = shapely.geometry.Polygon(h3.h3_to_geo_boundary(centroid_hex, geo_json=True))
  buffer_r = hex_max_distance(hex_polygon)
  
  dirty = polygon.boundary.buffer(0.5*buffer_r)
  
  original_set = to_h3(polygon)
  dirty_set = to_h3(dirty)
  
  result = [get_result_struct(hex_i, polygon, dirty_set) for hex_i in list(original_set.union(dirty_set))]
  
  return result

# COMMAND ----------

hexagon_layer_with_chips = neighborhoods_df.where(
  F.col("borough").like("Bronx")
).select(
  F.col("neighborhood").alias("id"), F.col("wkt_polygons")
).withColumn(
  "h3_hexes", polygon2H3_with_chips(F.col("wkt_polygons"), F.lit(9))
).select(
  F.explode("h3_hexes").alias("id"),
  F.lit(1).alias("count")
).select(
  F.col("id.hex").alias("id"),
  F.col("id.is_dirty").alias("is_dirty"),
  F.col("id.wkt_chip").alias("wkt_chip"),
  F.col("count")
).where(
  ~(F.col("is_dirty") & F.col("wkt_chip").isNull())
)

display(hexagon_layer_with_chips)

# COMMAND ----------

dirty_hexes_layer = hexagon_layer_with_chips.where(F.col("is_dirty")).select("id", "count").toPandas()
clean_hexes_layer = hexagon_layer_with_chips.where(~F.col("is_dirty")).select("id", "count").toPandas()
chip_polys = hexagon_layer_with_chips.where(F.col("wkt_chip").isNotNull()).select("id", "wkt_chip").toPandas()

# COMMAND ----------

m7 = KeplerGl(height=800, config=plot7_config)
m7.add_data(data=dirty_hexes_layer, name = "dirty")
m7.add_data(data=clean_hexes_layer, name = "clean")
m7.add_data(data=chip_polys, name = "chips")

displayHTML(m7._repr_html_())

# COMMAND ----------

import pandas as pd
import pyspark.sql.functions as F
df = spark.createDataFrame(pd.DataFrame([10,10]))
df = df.withColumn("x", F.array(*[F.lit(i) for i in range(0, 1000)]))
df.select(F.explode("x"))

# COMMAND ----------


