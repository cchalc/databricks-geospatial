# Databricks notebook source
# MAGIC %md
# MAGIC # Feature Store Demo

# COMMAND ----------

# MAGIC %md This notebook demonstrates a prototype of the Databricks Feature Store.
# MAGIC 
# MAGIC Let's use Feature Store to create a model to predict NYC Yellow Taxi fares. We will:
# MAGIC 
# MAGIC - Compute and write features
# MAGIC - Train a model using these features to predict fares
# MAGIC - Evaluate that model on a new batch of data using existing features, saved to Feature Store
# MAGIC - Publish features to an online store
# MAGIC - Lookup features from the online store to evaluate a model in real time

# COMMAND ----------

# MAGIC %md
# MAGIC ![fs](files/feature-store/flow_v3.png)

# COMMAND ----------

# MAGIC %md ## Feature Computation

# COMMAND ----------

# MAGIC %md ##### Setup & installation

# COMMAND ----------

default_wheel_path = (
    "/dbfs/feature_store/wheel/databricks_feature_store-0.3.0-py3-none-any.whl"
)
dbutils.widgets.text("fs_wheel", defaultValue=default_wheel_path)

wheel_path = dbutils.widgets.get("fs_wheel")

# COMMAND ----------

# MAGIC %pip install $wheel_path

# COMMAND ----------

# MAGIC %md
# MAGIC Install libraries needed to lookup feature values from the online store, which is backed by Aurora MySQL.

# COMMAND ----------

# MAGIC %pip install sqlalchemy PyMySQL

# COMMAND ----------

# MAGIC %md ##### Load the raw data used to compute features

# COMMAND ----------

raw_data = spark.read.table("feature_store_examples.demo_nyc_yellow_taxi_tiny")
display(raw_data)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC From the above mentioned taxi fares transactional data, we will be computing two groups of features based on trip's pickup and drop off zip codes.
# MAGIC 
# MAGIC ## Pickup features
# MAGIC 1. Count of trips (time window = 1 hour, sliding window = 15 minutes)
# MAGIC 1. Mean fare amount (time window = 1 hour, sliding window = 15 minutes)
# MAGIC 
# MAGIC ## Drop off features
# MAGIC 1. Count of trips (time window = 30 minutes)
# MAGIC 1. Does trip end on the weekend (custom feature using python code)
# MAGIC 
# MAGIC ![fs](files/feature-store/computation_v5.png)

# COMMAND ----------

# MAGIC %md ##### Helper functions

# COMMAND ----------

from databricks import feature_store
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, IntegerType, StringType
from pytz import timezone


@udf(returnType=IntegerType())
def is_weekend(dt):
    tz = "America/New_York"
    return int(dt.astimezone(timezone(tz)).weekday() >= 5)  # 5 = Saturday, 6 = Sunday
  
@udf(returnType=StringType())  
def partition_id(dt):
    # datetime -> "YYYY-MM"
    return f"{dt.year:04d}-{dt.month:02d}"


def filter_df_by_ts(df, ts_column, start_date, end_date):
    if ts_column and start_date:
        df = df.filter(col(ts_column) >= start_date)
    if ts_column and end_date:
        df = df.filter(col(ts_column) < end_date)
    return df


# COMMAND ----------

# MAGIC %md ##### Data scientist's custom code to compute features

# COMMAND ----------

# The @feature_store.feature_table decorator adds functionality to write feature values to the
# feature store.
@feature_store.feature_table
def pickup_features_fn(df, ts_column, start_date, end_date):
    """
    Computes the pickup_features feature group.
    To restrict features to a time range, pass in ts_column, start_date, and/or end_date as kwargs.
    """
    df = filter_df_by_ts(
        df, ts_column, start_date, end_date
    )
    pickupzip_features = (
        df.groupBy(
            "pickup_zip", window("tpep_pickup_datetime", "1 hour", "15 minutes")
        )  # 1 hour window, sliding every 15 minutes
        .agg(
            mean("fare_amount").alias("mean_fare_window_1h_pickup_zip"),
            count("*").alias("count_trips_window_1h_pickup_zip"),
        )
        .select(
            col("pickup_zip").alias("zip"),
            unix_timestamp(col("window.end")).alias("ts").cast(IntegerType()),
            partition_id(to_timestamp(col("window.end"))).alias("yyyy_mm"),
            col("mean_fare_window_1h_pickup_zip").cast(FloatType()),
            col("count_trips_window_1h_pickup_zip").cast(IntegerType()),
        )
    )
    return pickupzip_features
  
@feature_store.feature_table
def dropoff_features_fn(df, ts_column, start_date, end_date):
    """
    Computes the dropoff_features feature group.
    To restrict features to a time range, pass in ts_column, start_date, and/or end_date as kwargs.
    """
    df = filter_df_by_ts(
        df,  ts_column, start_date, end_date
    )
    dropoffzip_features = (
        df.groupBy("dropoff_zip", window("tpep_dropoff_datetime", "30 minute"))
        .agg(count("*").alias("count_trips_window_30m_dropoff_zip"))
        .select(
            col("dropoff_zip").alias("zip"),
            unix_timestamp(col("window.end")).alias("ts").cast(IntegerType()),
            partition_id(to_timestamp(col("window.end"))).alias("yyyy_mm"),
            col("count_trips_window_30m_dropoff_zip").cast(IntegerType()),
            is_weekend(col("window.end")).alias("dropoff_is_weekend"),
        )
    )
    return dropoffzip_features  

# COMMAND ----------

from datetime import datetime

pickup_features = pickup_features_fn(
    raw_data, ts_column="tpep_pickup_datetime", start_date=datetime(2015, 1, 1), end_date=datetime(2017, 1, 1)
)
dropoff_features = dropoff_features_fn(
    raw_data, ts_column="tpep_dropoff_datetime", start_date=datetime(2015, 1, 1), end_date=datetime(2017, 1, 1)
)

# COMMAND ----------

display(pickup_features)

# COMMAND ----------

# MAGIC %md
# MAGIC Use Feature Store library to create new feature tables

# COMMAND ----------

fs = feature_store.FeatureStoreClient()

# COMMAND ----------

# MAGIC %md
# MAGIC Use `create_feature_table` API to define schema and unique ID keys. If the optional `features_df` argument is passed, the API will also write the data to Feature Store.

# COMMAND ----------

sqlContext.setConf("spark.sql.shuffle.partitions", "5")

fs.create_feature_table(
    name="feature_store_demo.trip_pickup_features",
    keys=["zip", "ts"],
    features_df=pickup_features,
    partition_columns="yyyy_mm",
    description="Taxi Fares. Pickup Features",
)
fs.create_feature_table(
    name="feature_store_demo.trip_dropoff_features",
    keys=["zip", "ts"],
    features_df=dropoff_features,
    partition_columns="yyyy_mm",
    description="Taxi Fares. Dropoff Features",
)

# COMMAND ----------

# MAGIC %md
# MAGIC You can now discover your feature tables in the <a href="#feature-store/" target="_blank">Feature Store UI</a>.
# MAGIC 
# MAGIC Search by "trip_pickup_features" or "trip_dropoff_features" to view details such as table schema, metadata, data sources, producers, and online stores. You can also edit the description for the feature table, or configure permissions for a feature table via the dropdown icon next to the feature table name. 

# COMMAND ----------

# MAGIC %md ## Updating Features
# MAGIC 
# MAGIC You can schedule a notebook to periodically write features. 
# MAGIC 
# MAGIC Use the `compute_and_write` function to update the feature table values. This feature store function is an attribute of user specified functions that are decorated with `@feature_store.feature_table`.

# COMMAND ----------

# MAGIC %md
# MAGIC ![dbfs](files/feature-store/compute_and_write.png)

# COMMAND ----------

# The argments to pickup_features_fn are passed as a dictionary to the "input" parameter of compute_and_write.
pickup_features_fn.compute_and_write(
    input={
      'df': raw_data, 
      'ts_column': "tpep_pickup_datetime", 
      'start_date': datetime(2017, 1, 1),
      'end_date': datetime(2017, 1, 2),
    },
    feature_table_name="feature_store_demo.trip_pickup_features", 
    mode="merge"
)

dropoff_features_fn.compute_and_write(
    input={
      'df': raw_data, 
      'ts_column': "tpep_dropoff_datetime", 
      'start_date': datetime(2017, 1, 1),
      'end_date': datetime(2017, 1, 2),
    },
    feature_table_name="feature_store_demo.trip_dropoff_features",
    mode="merge"
)

# COMMAND ----------

# MAGIC %md When writing, both `merge` and `overwrite` modes are supported.
# MAGIC 
# MAGIC     dropoff_features_fn.compute_and_write(
# MAGIC         input={
# MAGIC           'df': raw_data, 
# MAGIC           'ts_column': "tpep_dropoff_datetime", 
# MAGIC           'start_date': datetime(2017, 1, 1),
# MAGIC           'end_date': datetime(2017, 1, 2),
# MAGIC         },
# MAGIC         feature_table_name="feature_store_demo.trip_dropoff_features",
# MAGIC         mode="overwrite"
# MAGIC     )
# MAGIC 
# MAGIC 
# MAGIC Data can also be streamed into Feature Store with `compute_and_write_streaming`, for example:
# MAGIC 
# MAGIC     dropoff_features_fn.compute_and_write_streaming(
# MAGIC         input={
# MAGIC           'df': streaming_input, 
# MAGIC           'ts_column': "tpep_dropoff_datetime", 
# MAGIC           'start_date': datetime(2017, 1, 1),
# MAGIC           'end_date': datetime(2017, 1, 2),
# MAGIC         },
# MAGIC         feature_table_name="feature_store_demo.trip_dropoff_features",
# MAGIC     )

# COMMAND ----------

# MAGIC %md Analysts can interact with Feature Store using SQL, for example:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(count_trips_window_30m_dropoff_zip) AS num_rides,
# MAGIC        dropoff_is_weekend
# MAGIC FROM   feature_store_demo.trip_dropoff_features
# MAGIC WHERE  dropoff_is_weekend IS NOT NULL
# MAGIC GROUP  BY dropoff_is_weekend;

# COMMAND ----------

# MAGIC %md ## Model Training
# MAGIC 
# MAGIC In this section, we train a LightGBM model to predict taxi fare using the pickup and dropoff features stored in Feature Store

# COMMAND ----------

# MAGIC %md ##### Helper functions

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import IntegerType
import math
from datetime import timedelta


def rounded_unix_timestamp(dt, num_minutes=15):
    """
    Ceilings datetime dt to interval num_minutes, then returns the unix timestamp.
    """
    nsecs = dt.minute * 60 + dt.second + dt.microsecond * 1e-6
    delta = math.ceil(nsecs / (60 * num_minutes)) * (60 * num_minutes) - nsecs
    return int((dt + timedelta(seconds=delta)).timestamp())


rounded_unix_timestamp_udf = udf(rounded_unix_timestamp, IntegerType())


def rounded_taxi_data(taxi_data_df):
    # Round the taxi data timestamp to 15 and 30 minute intervals so we can join with the pickup and dropoff features
    # respectively.
    taxi_data_df = (
        taxi_data_df.withColumn(
            "rounded_pickup_datetime",
            rounded_unix_timestamp_udf(taxi_data_df["tpep_pickup_datetime"], lit(15)),
        )
        .withColumn(
            "rounded_dropoff_datetime",
            rounded_unix_timestamp_udf(taxi_data_df["tpep_dropoff_datetime"], lit(30)),
        )
        .drop("tpep_pickup_datetime")
        .drop("tpep_dropoff_datetime")
    )
    taxi_data_df.createOrReplaceTempView("taxi_data")
    return taxi_data_df

# COMMAND ----------

from databricks.feature_store import FeatureLookup

dropoff_feature_lookups = [
    FeatureLookup( 
      table_name = "feature_store_demo.trip_dropoff_features",
      feature_name = "count_trips_window_30m_dropoff_zip",
      lookup_key = ["dropoff_zip", "rounded_dropoff_datetime"],
    ),
    FeatureLookup( 
      table_name = "feature_store_demo.trip_dropoff_features",
      feature_name = "dropoff_is_weekend",
      lookup_key = ["dropoff_zip", "rounded_dropoff_datetime"],
    ),
]

# COMMAND ----------

# MAGIC %md ##### Read taxi data for training

# COMMAND ----------

# Load data from 2015 for training
raw_taxi_data = spark.read.table(
    "feature_store_examples.demo_nyc_yellow_taxi_tiny"
).filter(f"YEAR(tpep_pickup_datetime) = {2015}")

taxi_data = rounded_taxi_data(raw_taxi_data)

# COMMAND ----------

# MAGIC %md
# MAGIC Load features from Feature Store for model training by creating a `FeatureLookup` for each needed feature

# COMMAND ----------

from databricks.feature_store import FeatureLookup
import mlflow

pickup_features_table = "feature_store_demo.trip_pickup_features"

pickup_feature_lookups = [
    FeatureLookup( 
      table_name = pickup_features_table,
      feature_name = "mean_fare_window_1h_pickup_zip",
      lookup_key = ["pickup_zip", "rounded_pickup_datetime"],
    ),
    FeatureLookup( 
      table_name = pickup_features_table,
      feature_name = "count_trips_window_1h_pickup_zip",
      lookup_key = ["pickup_zip", "rounded_pickup_datetime"],
    ),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ![join](files/feature-store/feature_lookup.png)

# COMMAND ----------

# MAGIC %md
# MAGIC Create a `TrainingSet` from the FeatureLookups. The `TrainingSet` is then transformed into a DataFrame to train on.
# MAGIC 
# MAGIC The `TrainingSet` will include all columns of taxi_data, as well as the features specified in the FeatureLookups. To avoid training on the "rounded_pickup_datetime" and "rounded_dropoff_datetime" columns, we pass those columns into the exclude_columns parameter.

# COMMAND ----------

mlflow.start_run()

training_set = fs.create_training_set(
  taxi_data,
  feature_lookups = pickup_feature_lookups + dropoff_feature_lookups,
  label = "fare_amount",
  exclude_columns = ["rounded_pickup_datetime", "rounded_dropoff_datetime"]
)

training_df = training_set.load_df()

# COMMAND ----------

display(training_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Train a LightGBM model on the data returned by `TrainingSet.to_df`, then log the model with `FeatureStoreClient.log_model`. The model will be packaged with feature metadata.

# COMMAND ----------

from sklearn.model_selection import train_test_split
from mlflow.tracking import MlflowClient


features_and_label = training_df.columns

# Collect data into a Pandas array for training
data = training_df.toPandas()[features_and_label]

train, test = train_test_split(data, random_state=123)
X_train = train.drop(["fare_amount"], axis=1)
X_test = test.drop(["fare_amount"], axis=1)
y_train = train.fare_amount
y_test = test.fare_amount

# Train a lightGBM model
import lightgbm as lgb
import mlflow.lightgbm
from mlflow.models.signature import infer_signature

mlflow.lightgbm.autolog()
train_lgb_dataset = lgb.Dataset(X_train, label=y_train.values)
test_lgb_dataset = lgb.Dataset(X_test, label=y_test.values)

param = {"num_leaves": 32, "objective": "regression", "metric": "rmse"}
num_rounds = 100

model = lgb.train(
  param, train_lgb_dataset, num_rounds
)

# COMMAND ----------

# Log the trained model with MLflow and package it with feature lookup information. 
fs.log_model(
  model,
  "model_packaged",
  flavor=mlflow.lightgbm,
  training_set=training_set,
  registered_model_name="demo_taxi_fare_packaged"
)

# COMMAND ----------

# MAGIC %md ##### Helper functions

# COMMAND ----------

def register_model(artifact_uri, model_name):
    client = MlflowClient()
    model_version = mlflow.register_model(artifact_uri, model_name)
    model_uri = f"models:/demo_taxi_fare/{model_version.version}"
    return model_uri

run_id = mlflow.active_run().info.run_id
model_uri = register_model(f"runs:/{run_id}/model", "demo_taxi_fare")

# COMMAND ----------

# MAGIC %md ## Batch Inference

# COMMAND ----------

# MAGIC %md Suppose another data scientist now wants to apply this model to a different batch of data.

# COMMAND ----------

raw_new_taxi_data = spark.read.table(
    "feature_store_examples.demo_nyc_yellow_taxi_tiny"
).filter(f"YEAR(tpep_pickup_datetime) = {2016}")

new_taxi_data = rounded_taxi_data(raw_new_taxi_data)

# COMMAND ----------

# MAGIC %md
# MAGIC Use `score_batch` API to evaluate the model on the batch of data, retrieving needed features from FeatureStore. 

# COMMAND ----------

import mlflow.pyfunc

model_uri = "models:/demo_taxi_fare_packaged/1"
with_predictions = fs.score_batch(model_uri, new_taxi_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ![join](files/feature-store/score_batch.png)

# COMMAND ----------

display(new_taxi_data)
display(with_predictions)

# COMMAND ----------

# MAGIC %md ## Publish features to an online store

# COMMAND ----------

# MAGIC %md ##### Helper functions

# COMMAND ----------

# Read Aurora database info from Databricks secret manager


def getSecret(key, scope="feature-store"):
    return dbutils.secrets.get(scope, key)


hostname = getSecret("aurora_hostname")
port = int(getSecret("aurora_port"))
user = getSecret(key="aurora_user")
password = getSecret(key="aurora_password")

# COMMAND ----------

# MAGIC %md ##### Specify the online store to publish a feature table to

# COMMAND ----------

from databricks.feature_store.online_store_spec import AmazonRdsMySqlSpec

online_store_spec = AmazonRdsMySqlSpec(hostname, port, user, password)

# COMMAND ----------

fs.publish_table("feature_store_demo.trip_pickup_features", online_store_spec)
fs.publish_table("feature_store_demo.trip_dropoff_features", online_store_spec)

# COMMAND ----------

# MAGIC %md Features can also be computed and published with Delta Structured Streaming to continuously update data in the key-value store.

# COMMAND ----------

# MAGIC %md ## Online Inference
# MAGIC 
# MAGIC Evaluate the model on transactional data by querying the online low-latency store for feature values.
# MAGIC 
# MAGIC Note: This section requires that you are in the Model Serving Private Preview. For more information see [MLflow Model Serving on Databricks](https://databricks.com/blog/2020/06/25/announcing-mlflow-model-serving-on-databricks.html).

# COMMAND ----------

# MAGIC %md ##### Helper functions

# COMMAND ----------

import sqlalchemy
from sqlalchemy import func, Table, MetaData
from sqlalchemy.orm import sessionmaker

from contextlib import contextmanager

# Set up connection to Aurora
engine = sqlalchemy.create_engine(
    f"mysql+pymysql://{user}:{password}@{hostname}:{port}/{online_database}"
)
Session = sessionmaker(bind=engine)


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def _fetch_features_for_key(sess, table_name, pk_cols, pk_values, feat_cols):
    pk_filter_phrase = " AND ".join([f"{pk} = :{pk}" for pk in pk_cols])
    select_feat_phrase = ", ".join(feat_cols)
    statement = sqlalchemy.sql.text(
        f"SELECT {select_feat_phrase} FROM {table_name} WHERE {pk_filter_phrase}"
    )
    statement_params = dict(zip(pk_cols, pk_values))
    sql_data = sess.execute(statement, statement_params)
    feat_values = sql_data.fetchone()
    if feat_values is None:
        return None
    return dict(zip(feat_cols, feat_values))


def lookup_online_features(feature_table_name, keys, feature_names=None):
    """
    This helper function fetches features from the online store.
    The FeatureStoreClient will later be updated to include this capability.
    """
    feature_table = fs.get_feature_table(feature_table_name)
    # Retrieve data from the first (and only) online store the feature table has been published to.
    online_store = feature_table.online_stores[0]
    online_table = Table(
        online_store.name.split(".")[1], MetaData(), autoload=True, autoload_with=engine
    )

    primary_key_columns = feature_table.keys
    feature_columns = feature_names if feature_names else online_store["features"]

    with session_scope() as session:
        # Fetch features for each key
        results = []
        for key in keys:
            feature_dict = _fetch_features_for_key(
                session, online_table.name, primary_key_columns, key, feature_columns
            )
            results += [feature_dict]
        return results


# COMMAND ----------

from collections import OrderedDict

PICKUP_FEATURE_NAMES = [
    "mean_fare_window_1h_pickup_zip",
    "count_trips_window_1h_pickup_zip",
]
DROPOFF_FEATURE_NAMES = ["count_trips_window_30m_dropoff_zip", "dropoff_is_weekend"]


def get_features_for_row(row):
    """
    A helper function to retrieve features for this row from MySQL.
    Returns an ordered dictionary which can be passed to the model for scoring.
    """
    pickup_row_key = [
        row.pickup_zip,
        rounded_unix_timestamp(row.tpep_pickup_datetime, 15),
    ]
    pickup_features = lookup_online_features(
        "feature_store_demo.trip_pickup_features",
        [pickup_row_key],
        feature_names=PICKUP_FEATURE_NAMES,
    )[0]
    dropoff_row_key = [
        row.dropoff_zip,
        rounded_unix_timestamp(row.tpep_dropoff_datetime, 30),
    ]
    dropoff_features = lookup_online_features(
        "feature_store_demo.trip_dropoff_features",
        [dropoff_row_key],
        feature_names=DROPOFF_FEATURE_NAMES,
    )[0]

    transactional_features = {
        k: row[k] for k in ["dropoff_zip", "pickup_zip", "trip_distance"]
    }
    features = {**transactional_features, **pickup_features, **dropoff_features}

    return OrderedDict(features.items())


# COMMAND ----------

# YOU SHOULD FILL IN THE BELOW STRING with the Model URL found on the Model Serving page.
model_endpoint = "https://dogfood.staging.cloud.databricks.com/model/demo_taxi_fare/1/invocations"

# COMMAND ----------

# MAGIC %md ##### Model serving
# MAGIC You can now serve the model from a Databricks cluster. Click "Models" in the left hand navigation bar. Search for the "demo_taxi_fare" model, click the "Serving" tab, and then "Enable Serving".
# MAGIC 
# MAGIC From the model serving page, use the Request text area to send a request like this to the REST API and get back a score.
# MAGIC 
# MAGIC     [
# MAGIC       {
# MAGIC         "count_trips_window_30m_dropoff_zip": 77,
# MAGIC         "count_trips_window_1h_pickup_zip": 546,
# MAGIC         "dropoff_is_weekend": 0,
# MAGIC         "dropoff_zip": 10199,
# MAGIC         "mean_fare_window_1h_pickup_zip": 10.326007326007327,
# MAGIC         "pickup_zip": 10017,
# MAGIC         "trip_distance": 1.6
# MAGIC       }
# MAGIC     ]
# MAGIC 
# MAGIC Save the Model URL on the model serving page to the "model_endpoint" variable below.

# COMMAND ----------

# MAGIC %md Next we'll retrieve the features for a particular row of data from Feature Store, then send a REST request to the model with these features to get back the predicted taxi fare.

# COMMAND ----------

row = raw_new_taxi_data.filter(
    (col("tpep_pickup_datetime") >= datetime(2016, 1, 1))
    & (col("tpep_pickup_datetime") < datetime(2016, 1, 2))
).rdd.takeSample(False, 1, seed=345)[0]

# COMMAND ----------

features = get_features_for_row(row)

# COMMAND ----------

import requests

from databricks_cli.configure.provider import get_config_provider

TOKEN = get_config_provider().get_config().token


def get_score(features):
    response = requests.post(
        model_endpoint,
        headers={
            "Content-Type": "application/json; format=pandas-records",
            "Authorization": "Bearer %s" % TOKEN,
        },
        json=[features],
    )
    return response.json()


score = get_score(features)
print(f"Predicted Fare: {score}")
