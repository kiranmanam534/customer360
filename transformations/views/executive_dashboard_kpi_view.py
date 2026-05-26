from pyspark import pipelines as dp

import os
import sys

base_path = os.path.abspath(os.path.join(os.getcwd(),'..'))
sys.path.append(base_path)

from pyspark.sql import functions as F
from utilities.constants import CATALOG,GOLD_SCHEMA


@dp.temporary_view(
    name = "executive_dashboard_kpi_view",
    comment = "This is the KPI table for the Executive Dashboard."
)
def executive_dashboard_kpi_view():
    df_360 = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.customer360") 

    df_kpi = (
        df_360
        .groupBy()
        .agg(
            F.countDistinct("order_id").alias("Total_orders"),
            F.sum("order_amount").alias("Total_Amount"),
            F.sum("payment_amount").alias("Total_Payment"),
            F.sum(F.when(F.col("payment_status") == "success", 1).otherwise(0)).alias("Total_success_payments"),
            F.sum(F.when(F.col("payment_status") == "failed", 1).otherwise(0)).alias("Total_failed_payments"),
            F.countDistinct("device_type").alias("Total_Devices"),
            F.countDistinct("city").alias("Total_Cities"),
            F.countDistinct("state").alias("Total_States"),
            F.countDistinct("country").alias("Total_Countries"),
            F.countDistinct("zipcode").alias("Total_Zipcodes"),
            F.countDistinct("ticket_id").alias("Total_Tickets"),
            F.countDistinct("issue_type").alias("Total_Issues"),
            F.countDistinct("session_id").alias("Total_Sessions"),
            F.countDistinct("page_viewed").alias("Total_Pages")
        )
    )

    return df_kpi
 