from pyspark import pipelines as dp

from pyspark.sql.functions import *

import os
import sys

base_path = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.append(base_path)

from utilities.config import config
from utilities.constants import CATALOG, SILVER_SCHEMA


@dp.temporary_view(
    name="active_locations",
    comment="Active locations"
)
def active_locations():
    return (
        spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.locations_scd2")
        .filter(col("__END_AT").isNull())
        .select('location_id',"city","state","country","zipcode")
    )


