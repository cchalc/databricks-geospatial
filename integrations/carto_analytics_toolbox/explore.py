# Databricks notebook source
# MAGIC %md 
# MAGIC # Explore: CARTO Analytics Toolbox
# MAGIC
# MAGIC > With CARTO, users will now have access to low-code workflows, vector + raster data processing and analysis, map-making plus app development tools - all running against their Databricks Lakehouse! And, a not so small bonus is that CARTO has made their Analytics Toolbox FREE for all Databricks users - [blog](https://carto.com/blog/carto-for-databricks-true-native-geospatial-for-the-lakehouse)
# MAGIC
# MAGIC _Download and instructions [here](https://docs.carto.com/data-and-analysis/analytics-toolbox-for-databricks/getting-access)._
# MAGIC
# MAGIC
# MAGIC * Running on DBR 15.4 LTS
# MAGIC * [carto_sql_init.sh](/Workspace/Users/mjohns@databricks.com/Partners/CARTO/20241101_CARTO_Analytics_Toolbox/carto-config/carto_sql_init.sh) cluster init script (loads [JAR](/Workspace/Users/mjohns@databricks.com/Partners/CARTO/20241101_CARTO_Analytics_Toolbox/carto-config/analytics-toolbox-1.0.0.jar))
# MAGIC * Supports Assigned Access or Shared Access clusters
# MAGIC
# MAGIC __Cluster Config [Spark]__
# MAGIC
# MAGIC ```
# MAGIC spark.sql.extensions com.carto.analytics.toolbox.sql.SparkExtension
# MAGIC spark.databricks.<masked> true # <- ask us about joining the private preview
# MAGIC spark.databricks.geo.st.enabled true
# MAGIC ```
# MAGIC
# MAGIC __Cluster Environment Variables__
# MAGIC
# MAGIC ```
# MAGIC PYSPARK_PYTHON=/databricks/python3/bin/python
# MAGIC SCALAPY_PYTHON_LIBRARY=python3.11
# MAGIC SCALAPY_PYTHON_PROGRAMNAME=/databricks/python3/bin/python
# MAGIC CARTO_JAR_LOCATION=/Workspace/Users/mjohns@databricks.com/Partners/CARTO/20241101_CARTO_Analytics_Toolbox/carto-config/analytics-toolbox-1.0.0.jar
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC __Author:__ Michael Johns <mjohns@databricks.com> | _Last Modified:_ 04 NOV 2024

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

spark.catalog.currentCatalog()

# COMMAND ----------

def filter_functions_by_classpath(class_path, catalog=None, schema=None, debug=1):
  """
  Used to identify functions by provider.
  """
  import json

  curr_catalog = spark.catalog.currentCatalog()
  if catalog is not None:
    spark.catalog.setCurrentCatalog(catalog)

  functions = []  
  try:
    schema_token = ""
    if schema is not None:
      schema_token = f" in {schema}"
    all_functions = [json.loads(j)['function'] for j in sql(f"""show functions{schema_token}""").toJSON().collect()]
    for f in all_functions:
      try:
        for d in [json.loads(j) for j in sql(f"""describe function extended {f}""").toJSON().collect()]:
          for k,v in d.items():
            if v.startswith(f"Class: {class_path}"):
              functions.append(f)
      except:
        debug > 1 and print(f"... skipping {f} (could not describe)")
        continue
    debug > 0 and print(f"count? {len(functions)} -> {functions}")
  finally:
    if catalog is not None:
      spark.catalog.setCurrentCatalog(curr_catalog)
  return functions

# COMMAND ----------

def get_databricks_st_functions(catalog=None, schema=None, debug=1):
  """
  66 databricks_st functions.
  """
  return filter_functions_by_classpath('com.databricks.sql.catalyst.expressions.st.', catalog=catalog, schema=schema, debug=debug)

# COMMAND ----------

def get_databricks_h3_functions(catalog=None, schema=None, debug=1):
  """
  37 databricks_h3 functions.
  """
  return filter_functions_by_classpath('com.databricks.sql.catalyst.expressions.h3.', catalog=catalog, schema=schema, debug=debug)

# COMMAND ----------

def get_sedona_functions(class_path_only=False, catalog=None, schema=None, debug=1):
  """
  231 sedona functions (227 if class_path_only = True)
  """
  if class_path_only:
    return filter_functions_by_classpath('org.apache.spark.sql.sedona_sql.expressions.', catalog=catalog, schema=schema, debug=debug)
  else:
    import json

    curr_catalog = spark.catalog.currentCatalog()
    if catalog is not None:
        spark.catalog.setCurrentCatalog(catalog)
    
    functions = []
    try:
      schema_token = ""
      if schema is not None:
        schema_token = f" in {schema}"
      
      functions = [
        json.loads(j)['function'] for j in sql(f"show functions{schema_token} like 'sedona_*'").toJSON().collect()
      ]
      debug > 0 and print(f"count? {len(functions)} -> {functions}")
    finally:
      spark.catalog.setCurrentCatalog(curr_catalog)
    return functions

# COMMAND ----------

def get_carto_functions(catalog=None, schema=None, debug=1):
  """
  31 carto functions.
  """
  return filter_functions_by_classpath('com.carto.analytics.toolbox.', catalog=catalog, schema=schema, debug=debug)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Provided `ST_` Functions [66]

# COMMAND ----------

databricks_st_functions = get_databricks_st_functions()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Provided `H3_` Functions [37]

# COMMAND ----------

databricks_h3_functions = get_databricks_h3_functions()

# COMMAND ----------

# MAGIC %md
# MAGIC ## CARTO Provided Functions [31]

# COMMAND ----------

carto_functions = get_carto_functions()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sedona Functions [231]
# MAGIC
# MAGIC The following are UDFs ... the rest are provided by 'org.apache.spark.sql.sedona_sql.expressions.*'
# MAGIC
# MAGIC <p/>
# MAGIC
# MAGIC ```
# MAGIC sedona_rs_union_aggr
# MAGIC sedona_st_envelope_aggr
# MAGIC sedona_st_intersection_aggr
# MAGIC sedona_st_union_aggr
# MAGIC ```

# COMMAND ----------

sedona_functions = get_sedona_functions()
