from pyspark import pipelines as dp
from pyspark.sql import functions as F

import os
import sys

base_path = os.path.abspath(os.path.join(os.getcwd(), '..'))
sys.path.append(base_path)

from utilities.constants import CATALOG, GOLD_SCHEMA


@dp.materialized_view(
    name = f"{CATALOG}.{GOLD_SCHEMA}.support_sla_analytics",
    comment="This is a materialized view of device_type"
)
def support_sla_analytics():
    return spark.read.table(f"support_sla_analytics_view")

