

from pyspark import pipelines as dp

from pyspark.sql.functions import *

import sys
import os

base_path = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.append(base_path)

from utilities.config import config
from utilities.constants import RAW_SCHEMA,BRONZE_SCHEMA,CATALOG
from utilities.spark_config import read_stream_autoloader



@dp.table(
    name=f"{CATALOG}.{BRONZE_SCHEMA}.orders",
    comment="Raw orders ingestion"
    
)
def bronze_orders():
    path = config.get_path(RAW_SCHEMA,"orders")
    df = read_stream_autoloader(
        spark=spark,
        source_path=path
    )

    df.withColumn('created_at',current_timestamp()).withColumn('updated_at', current_timestamp())
    
    return df    
