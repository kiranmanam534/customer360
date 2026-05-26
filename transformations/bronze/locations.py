from pyspark import pipelines as dp
from pyspark.sql.functions import *

import os
import sys

base_path = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.append(base_path)

from utilities.constants import *
from utilities.spark_config import read_stream_autoloader_dynamic
from utilities.config import config


@dp.table(
    name=f"{CATALOG}.{BRONZE_SCHEMA}.locations",
    comment="Locations Bronze Table"
)
def locations():

    path = config.get_path(RAW_SCHEMA, "locations")

    options = {
        "cloudFiles.format": "csv",
        "header": "true",
        "inferSchema": "true",

        # IMPORTANT
        # tracks schema evolution
        "cloudFiles.schemaLocation":"abfss://customer360dp@kmstorage9490.dfs.core.windows.net/schema/locations"
    }

    df = read_stream_autoloader_dynamic(
        spark=spark,
        path=path,
        options=options
    )

   
    # =================================================
    # CONVERT TO TIMESTAMP
    # =================================================

    df = df.withColumn(
        "last_updated",
        to_timestamp(col("last_updated"))
    )

    return df