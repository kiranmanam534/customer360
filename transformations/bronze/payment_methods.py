from pyspark import pipelines as dp

from pyspark.sql.functions import *

import os
import sys

base_path = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.append(base_path)


from utilities.constants import CATALOG,BRONZE_SCHEMA,RAW_SCHEMA
from utilities.config import config
from utilities.spark_config import read_stream_autoloader_dynamic


@dp.table(
    name = f"{CATALOG}.{BRONZE_SCHEMA}.payments_methods",
    comment="Payments methods"
)
def payements_methods():

    path = config.get_path(RAW_SCHEMA, "payments_methods")

    options = {
        "cloudFiles.format": "csv",
        "header": "true",
        "inferSchema": "true",
        # IMPORTANT
        # tracks schema evolution
        # "cloudFiles.schemaLocation":"abfss://customer360dp@kmstorage9490.dfs.core.windows.net/schema/payments_methods"
    }

    df = read_stream_autoloader_dynamic(spark,path=path,options=options)
    df = df.withColumn(
        "last_updated",
        to_timestamp(col("last_updated"))
    )
    return df