from pyspark import pipelines as dp

import os
import sys

base_path = os.path.abspath(os.path.join(os.getcwd(),'..'))
sys.path.append(base_path)

from pyspark.sql import functions as F
from utilities.constants import CATALOG,GOLD_SCHEMA



@dp.temporary_view(
    name="customer_360_profile_view"
)
def customer_360_profile_view():
    df = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.customer360")

    return (
        df.groupBy("customer_id", "name", "email", "gender", "city", "state", "country")
          .agg(
              F.countDistinct("order_id").alias("total_orders"),
              F.sum("order_amount").alias("lifetime_value"),
              F.max("order_date").alias("last_order_date"),
              F.max("session_time").alias("last_session_time"),
              F.countDistinct("ticket_id").alias("ticket_count")
          )
    )