# write device analytics
from pyspark import pipelines as dp
from pyspark.sql import functions as F

import os
import sys

base_path = os.path.abspath(os.path.join(os.getcwd(), '..'))
sys.path.append(base_path)

from utilities.constants import CATALOG, GOLD_SCHEMA


@dp.temporary_view(
    name="device_analytics_view",
    comment="Device-level customer engagement, commerce, and support analytics"
)
def device_analytics_view():
    df = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.customer360")

    df = df.withColumn("device_type", F.coalesce(F.col("device_type"), F.lit("Unknown")))

    return (
        df.groupBy("device_type")
          .agg(
              F.countDistinct("customer_id").alias("total_customers"),
              F.countDistinct("session_id").alias("total_sessions"),
              F.countDistinct("order_id").alias("total_orders"),
              F.round(F.sum("order_amount"), 2).alias("total_order_amount"),
              F.round(F.sum("payment_amount"), 2).alias("total_payment_amount"),
              F.sum(F.when(F.lower(F.col("payment_status")) == "success", 1).otherwise(0)).alias("successful_payments"),
              F.sum(F.when(F.lower(F.col("payment_status")) == "failed", 1).otherwise(0)).alias("failed_payments"),
              F.countDistinct("ticket_id").alias("total_tickets"),
              F.countDistinct("page_viewed").alias("total_pages_viewed"),
              F.max("session_time").alias("last_session_date")
          )
          .withColumn(
              "avg_order_value",
              F.round(
                  F.when(F.col("total_orders") > 0, F.col("total_order_amount") / F.col("total_orders")),
                  2
              )
          )
          .withColumn(
              "orders_per_session",
              F.round(
                  F.when(F.col("total_sessions") > 0, F.col("total_orders") / F.col("total_sessions")),
                  2
              )
          )
    )
