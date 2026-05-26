

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
    name=f"{CATALOG}.{BRONZE_SCHEMA}.support_tickets",
    comment="Raw support_tickets ingestion"
    
)
def bronze_support_tickets():
    path = config.get_path(RAW_SCHEMA,"support_tickets")
    df = read_stream_autoloader(
        spark=spark,
        source_path=path
    )

    df.withColumn('created_at',current_timestamp()).withColumn('updated_at', current_timestamp())
    
    return df    
