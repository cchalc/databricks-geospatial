# Databricks notebook source
# MAGIC %fs ls /databricks/cjc

# COMMAND ----------

library(SparkR)

# COMMAND ----------

install.packages('/dbfs/databricks/cjc/sparkrMosaic_0.2.1.tar.gz', repos=NULL)

# COMMAND ----------

library(sparkrMosaic)

# COMMAND ----------

# MAGIC %md Can do the same for sparklyr
# MAGIC 
# MAGIC sparklyr is preinstalled on Databricks
# MAGIC ```
# MAGIC library(sparklyr)
# MAGIC 
# MAGIC sparklyr_mosaic_package_path = '/Users/<my-user-name>/sparklyrMosaic.tar.gz'
# MAGIC install.packages(sparklyr_mosaic_package_path, repos=NULL)
# MAGIC library(sparklyrMosaic)
# MAGIC ```

# COMMAND ----------


