from pyspark import pipelines as dp
from pyspark.sql.functions import col, coalesce, try_to_date, initcap, when

import os
import sys

base_path = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.append(base_path)

from utilities.constants import CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA

@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.orders_clean",
    comment="orders with cleaned up names and addresses"
)
def orders_clean():
    df = spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.orders")
    df = df.withColumn(
        "order_date",
        coalesce(
            try_to_date(col("order_date"), "yyyy/MM/dd"),
            try_to_date(col("order_date"), "dd-MM-yyyy"),
            try_to_date(col("order_date"), "yyyy-MM-dd"),
            try_to_date(col("order_date"), "yyyyMMdd"),
            try_to_date(col("order_date"), "MM-dd-yyyy"),
            try_to_date(col("order_date"), "dd/MM/yyyy"),
        )
    )
    df = df.withColumn("status", initcap(col("status")))
    df = df.withColumn("amount", col("amount").cast("double"))
    df = df.withColumn(
        "amount",
        when(col("amount").isNull(), 0)
        .when(col("amount") < 0, 0)
        .otherwise(col("amount"))
    )
    df = df.dropDuplicates(["order_id"])
    df = df.dropna(subset=["order_id", "customer_id", "order_date"])
    df = df.withColumn("customer_id", col("customer_id").cast("int"))
    df = df.withColumn("order_id", col("order_id").cast("int"))
    df = df.drop("_rescued_data")
    return df