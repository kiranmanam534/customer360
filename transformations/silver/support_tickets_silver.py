
from pyspark import pipelines as dp
from pyspark.sql.functions import col, coalesce, try_to_date, initcap, when, lower

import os
import sys

base_path = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.append(base_path)

from utilities.constants import CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA

@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.support_tickets_clean",
    comment="support_tickets with cleaned up names and addresses"
)
def support_tickets_clean():
    df = spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.support_tickets")
    df = df.withColumn(
        "ticket_date",
        coalesce(
            try_to_date(col("ticket_date"), "yyyy/MM/dd"),
            try_to_date(col("ticket_date"), "dd-MM-yyyy"),
            try_to_date(col("ticket_date"), "yyyy-MM-dd"),
            try_to_date(col("ticket_date"), "yyyyMMdd"),
            try_to_date(col("ticket_date"), "MM-dd-yyyy"),
            try_to_date(col("ticket_date"), "dd/MM/yyyy"),
        )
    )
    df = df.withColumn(
        "issue_type",
        initcap(
            when(
                lower(col("issue_type").cast("string")) == "na",
                "Unknown"
            ).otherwise(col("issue_type"))
        )
    )
    df = df.withColumn('resolution_status', initcap(col("resolution_status")))
    df = df.dropDuplicates(["ticket_id", "customer_id"])
    df = df.dropna(subset=["ticket_id", "customer_id"])
    df = df.drop("_rescued_data")
    df = df.withColumn("customer_id", col("customer_id").cast("int"))
    return df