# import sys
# import os

# base_path = os.path.abspath(os.path.join(os.getcwd(), ".."))
# sys.path.append(base_path)

# from config import config
# from constants import *
from pyspark.sql import DataFrame


def read_stream_autoloader_dynamic(spark, path, file_format="csv", options=None):
    reader = spark.readStream.format("cloudFiles").option("cloudFiles.format", file_format)
    if options:
        for k, v in options.items():
            reader = reader.option(k, v)
    df = reader.load(path)
    return df


def read_batch(spark, path, file_format="csv", options=None):
    reader = spark.read.format(file_format)
    if options:
        for k, v in options.items():
            reader = reader.option(k, v)
    df = reader.load(path)
    return df


def read_stream_autoloader(
    spark,
    source_path: str="",
    schema_location: str="",
    file_format: str = "csv",
    infer_column_types: bool = True,
    schema_evolution_mode: str = "rescue",
    max_files_per_trigger: int = 100,
    header: bool = True
) -> DataFrame:


    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", file_format)
        # .option("cloudFiles.schemaLocation", schema_location)
        # .option("cloudFiles.inferColumnTypes", str(infer_column_types).lower())
        .option("cloudFiles.schemaEvolutionMode", schema_evolution_mode)
        .option("cloudFiles.maxFilesPerTrigger", max_files_per_trigger)
        .option("header", str(header).lower())
        .load(source_path)
    )


def write_stream_table(
    df: DataFrame,
    table_name: str,
    output_mode: str = "append",
    trigger_available_now: bool = True,
    checkpoint_path: str="",
):
    writer = (
        df.writeStream
        .format("delta")
        .outputMode(output_mode)
        # .option("checkpointLocation", checkpoint_path)
    )

    if trigger_available_now:
        writer = writer.trigger(availableNow=True)

    return writer.toTable(table_name)    