
from pyspark import pipelines as dp
from pyspark.sql.functions import *

import os
import sys

base_path = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.append(base_path)

from utilities.constants import CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA


@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.payments_clean",
    comment="payments with cleaned up names and addresses"
)
def payments_clean():
    df = spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.payments")
    payments_methods_df = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.payment_methods_scd1")

    df = df.drop("_rescued_data")
    df = df.withColumn(
        "payment_date",
        coalesce(
            try_to_date(col("payment_date"), "yyyy/MM/dd"),
            try_to_date(col("payment_date"), "dd-MM-yyyy"),
            try_to_date(col("payment_date"), "yyyy-MM-dd"),
            try_to_date(col("payment_date"), "yyyyMMdd"),
            try_to_date(col("payment_date"), "MM-dd-yyyy"),
            try_to_date(col("payment_date"), "dd/MM/yyyy"),
        )
    )
    df = df.join(payments_methods_df.alias("p"), df.payment_method == col("p.payment_id"), "inner") \
        .select(df.payment_id, df.customer_id, df.payment_date, col("p.payment_method"), df.payment_status, df.amount)

    df = df.withColumn("payment_method", initcap(col("payment_method")))
    df = df.withColumn("payment_status", initcap(when(col("payment_status").isNull(), "Failed").otherwise(col("payment_status"))))
    df = df.withColumn("amount", col("amount").cast("double"))
    df = df.withColumn("amount", when(col("amount").isNull(), 0).when(col("amount") < 0, 0).otherwise(col("amount")))
    df = df.dropDuplicates(["payment_id", "customer_id"])
    df = df.dropna(subset=["payment_id", "customer_id"])
    df = df.withColumn("customer_id", col("customer_id").cast("int"))
    return df
