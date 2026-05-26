from pyspark import pipelines as dp
from pyspark.sql.functions import col, coalesce, try_to_date, lower, initcap

import os
import sys

base_path = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.append(base_path)

from utilities.constants import CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA

@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.web_activities_clean",
    comment="web_activities with cleaned up names and addresses"
)
def web_activities_clean():
    df = spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.web_activities")
    df = df.withColumn(
        "session_time",
        coalesce(
            try_to_date(col("session_time"), "yyyy/MM/dd"),
            try_to_date(col("session_time"), "dd-MM-yyyy"),
            try_to_date(col("session_time"), "yyyy-MM-dd"),
            try_to_date(col("session_time"), "yyyyMMdd"),
            try_to_date(col("session_time"), "MM-dd-yyyy"),
            try_to_date(col("session_time"), "dd/MM/yyyy"),
        )
    )
    df = df.withColumn('page_viewed', lower(col('page_viewed'))) \
           .withColumn('device_type', initcap(col('device_type')))
    df = df.dropDuplicates(["session_id", "customer_id"]) \
           .dropna(subset=["session_id", "customer_id"])
    df = df.drop("_rescued_data")
    df = df.withColumn("customer_id", col("customer_id").cast("int"))
    return df