from pyspark import pipelines as dp

from pyspark.sql import Window
from pyspark.sql.functions import col, row_number

import os
import sys

base_path = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.append(base_path)

from utilities.constants import CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA


@dp.temporary_view(
    name="locations_snapshot_dedup",
    comment="Deduplicated locations snapshot for Auto CDC"
)
def locations_snapshot_dedup():
    window_spec = Window.partitionBy("location_id").orderBy(col("ingestion_ts").desc())
    return (
        spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.locations")
        .filter(col("location_id").isNotNull())
        .withColumn("_row_num", row_number().over(window_spec))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )


dp.create_streaming_table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.locations_scd2",
    comment="SCD Type 2 history for locations"
)


dp.create_auto_cdc_from_snapshot_flow(
    target=f"{CATALOG}.{SILVER_SCHEMA}.locations_scd2",
    source="locations_snapshot_dedup",
    keys=["location_id"],
    stored_as_scd_type=2
)
