from pyspark import pipelines as dp
from pyspark.sql import functions as F

import os
import sys

base_path = os.path.abspath(os.path.join(os.getcwd(), '..'))
sys.path.append(base_path)

from utilities.constants import CATALOG, GOLD_SCHEMA


@dp.materialized_view(
    name = f"{CATALOG}.{GOLD_SCHEMA}.executive_dashboard_kpi",
    comment="This is a materialized view of device_type"
)
def get_executive_dashboard_kpi():
    return spark.read.table(f"executive_dashboard_kpi_view")

