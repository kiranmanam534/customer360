from pyspark import pipelines as dp
from pyspark.sql import functions as F

import os
import sys

base_path = os.path.abspath(os.path.join(os.getcwd(), '..'))
sys.path.append(base_path)

from utilities.constants import CATALOG, GOLD_SCHEMA


@dp.temporary_view(
    name="fraud_analytics_view",
    comment="Customer and device-level fraud risk indicators derived from customer360 activity"
)
def fraud_analytics_view():
    df = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.customer360")

    base_df = (
        df.select(
            "customer_id",
            "order_id",
            "payment_id",
            "session_id",
            "ticket_id",
            "device_type",
            "order_amount",
            "payment_amount",
            "payment_status",
            "order_date",
            "payment_date",
            "resolution_status"
        )
        .dropDuplicates()
        .withColumn("device_type", F.coalesce(F.col("device_type"), F.lit("Unknown")))
        .withColumn("payment_status_normalized", F.lower(F.coalesce(F.col("payment_status"), F.lit("unknown"))))
        .withColumn("resolution_status_normalized", F.lower(F.coalesce(F.col("resolution_status"), F.lit("unknown"))))
        .withColumn("high_value_order_flag", F.when(F.coalesce(F.col("order_amount"), F.lit(0.0)) >= 1000, 1).otherwise(0))
        .withColumn(
            "payment_mismatch_flag",
            F.when(
                F.col("order_amount").isNotNull()
                & F.col("payment_amount").isNotNull()
                & (F.abs(F.col("order_amount") - F.col("payment_amount")) > 0.01),
                1
            ).otherwise(0)
        )
        .withColumn("failed_payment_flag", F.when(F.col("payment_status_normalized") == "failed", 1).otherwise(0))
        .withColumn(
            "unresolved_ticket_flag",
            F.when(
                F.col("ticket_id").isNotNull()
                & (F.col("resolution_status_normalized") != "resolved"),
                1
            ).otherwise(0)
        )
        .withColumn(
            "same_day_order_payment_flag",
            F.when(
                F.col("order_date").isNotNull()
                & F.col("payment_date").isNotNull()
                & (F.col("order_date") == F.col("payment_date")),
                1
            ).otherwise(0)
        )
    )

    return (
        base_df.groupBy("customer_id", "device_type")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.countDistinct("payment_id").alias("total_payments"),
            F.countDistinct("session_id").alias("total_sessions"),
            F.countDistinct("ticket_id").alias("total_tickets"),
            F.round(F.sum("order_amount"), 2).alias("total_order_amount"),
            F.round(F.sum("payment_amount"), 2).alias("total_payment_amount"),
            F.sum("high_value_order_flag").alias("high_value_order_events"),
            F.sum("payment_mismatch_flag").alias("payment_mismatch_events"),
            F.sum("failed_payment_flag").alias("failed_payment_events"),
            F.sum("unresolved_ticket_flag").alias("unresolved_ticket_events"),
            F.sum("same_day_order_payment_flag").alias("same_day_order_payment_events"),
            F.max("order_date").alias("latest_order_date"),
            F.max("payment_date").alias("latest_payment_date")
        )
        .withColumn(
            "fraud_risk_score",
            F.col("high_value_order_events")
            + (F.col("payment_mismatch_events") * F.lit(2))
            + (F.col("failed_payment_events") * F.lit(2))
            + F.col("unresolved_ticket_events")
            + F.col("same_day_order_payment_events")
        )
        .withColumn(
            "fraud_risk_level",
            F.when(F.col("fraud_risk_score") >= 8, "High")
            .when(F.col("fraud_risk_score") >= 4, "Medium")
            .otherwise("Low")
        )
    )
