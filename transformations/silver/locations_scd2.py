from pyspark import pipelines as dp
from pyspark.sql.functions import col

import os
import sys

base_path = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.append(base_path)

from utilities.constants import *


# =====================================================
# CREATE STREAMING TABLE
# =====================================================

dp.create_streaming_table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.locations_scd2",
    comment="Locations SCD Type 2 Table"
)

# =====================================================
# AUTO CDC SCD2
# =====================================================

dp.create_auto_cdc_flow(
    target=f"{CATALOG}.{SILVER_SCHEMA}.locations_scd2",
    source=f"{CATALOG}.{BRONZE_SCHEMA}.locations",
    keys=["location_id"],
    sequence_by=col("last_updated"),
    stored_as_scd_type=2
)