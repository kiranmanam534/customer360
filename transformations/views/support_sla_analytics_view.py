from pyspark import pipelines as dp
from pyspark.sql import functions as F

import os
import sys

base_path = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.append(base_path)

from utilities.constants import CATALOG, GOLD_SCHEMA

SLA_DAYS_THRESHOLD = 7


@dp.temporary_view(
    name="support_sla_analytics_view",
    comment="Support ticket SLA analytics: resolution status, SLA breach detection, and time-based breakdowns"
)
def support_sla_analytics_view():
    df = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.customer360")

    df = df.withColumn("is_resolved", F.col("resolution_status") == "Resolved")
    df = df.withColumn("days_open", F.datediff(F.current_date(), F.col("ticket_date")))
    df = df.withColumn(
        "sla_status",
        F.when(F.col("is_resolved"), "Met")
         .when(F.col("days_open") <= SLA_DAYS_THRESHOLD, "On Track")
         .otherwise("Breached")
    )
    df = df.withColumn("ticket_year", F.year(F.col("ticket_date")))
    df = df.withColumn("ticket_month", F.month(F.col("ticket_date")))

    return df
