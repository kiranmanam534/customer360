
from pyspark import pipelines as dp

from pyspark.sql.functions import *


import os
import sys


base_path = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.append(base_path)

from utilities.constants import CATALOG,BRONZE_SCHEMA,SILVER_SCHEMA



@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.customers_clean",
    comment="Customers with cleaned up names and addresses"
)

def customers_clean():

    df = spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.customers")
    location_df = spark.read.table("active_locations")
    
    df = (
        df.withColumnRenamed("EMAIL", "email")
        .withColumn("customer_id", trim(col("customer_id")))
        .withColumn("location_id", trim(col("location_id")))
        .withColumn("email", lower(trim(col("email"))))
        .withColumn("name", initcap(regexp_replace(trim(col("name")), "\\s+", " ")))
        .withColumn("gender", upper(trim(col("gender"))))
        .withColumn(
            "gender",
            when(col("gender").isin("F", "FEMALE"), "Female")
            .when(col("gender").isin("M", "MALE"), "Male")
            .when(col("gender").isNull() | (col("gender") == ""), "Unknown")
            .otherwise("Unknown")
        )
        .withColumn(
            "dob",
            coalesce(
                try_to_date(trim(col("dob")), "yyyy/MM/dd"),
                try_to_date(trim(col("dob")), "dd-MM-yyyy"),
                try_to_date(trim(col("dob")), "yyyy-MM-dd")
            )
        )
        .drop("_rescued_data")
    )

    df = df.dropna(subset=["customer_id", "email", "name", "location_id"])
    df = df.filter(col("name") != "")
    df = df.filter(col("email").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"))
    df = df.filter(col("dob").isNull() | ((col("dob") >= lit("1900-01-01").cast("date")) & (col("dob") <= current_date())))
    df = df.dropDuplicates(["customer_id"])

    df = df.join(location_df,df.location_id.cast("int") == location_df.location_id,'inner')\
        .select("customer_id","name","email","gender","dob","city","state","country","zipcode")

    return df