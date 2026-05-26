from pyspark import pipelines as dp
from pyspark.sql.functions import col, coalesce, try_to_date, lower, initcap

import os
import sys

base_path = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.append(base_path)

from utilities.constants import CATALOG, GOLD_SCHEMA, SILVER_SCHEMA

@dp.materialized_view(
    name=f"{CATALOG}.{GOLD_SCHEMA}.customer360",
    comment="Customer 360 view"
)
def customer360():
    cust_df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.customers_clean")
    ord_df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.orders_clean")
    pay_df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.payments_clean")
    tickets_df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.support_tickets_clean")
    web_df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.web_activities_clean")
    customer360_df = cust_df.alias('c').join(ord_df.alias('o'), 'customer_id','left')\
        .join(pay_df.alias('p'), 'customer_id','left')\
        .join(tickets_df.alias('t'), 'customer_id','left')\
        .join(web_df.alias('w'), 'customer_id')\
        .select('c.customer_id','c.name','c.email','c.gender','c.dob','c.city','c.state','c.country','c.zipcode','o.order_id','o.order_date',col('o.amount').alias('order_amount'),col('status').alias('order_status'),'payment_id','payment_date','payment_method','payment_status',col('p.amount').alias('payment_amount'),'ticket_id','issue_type','ticket_date','resolution_status','session_id','page_viewed','session_time','device_type')
    # Clean the data: drop rows with any nulls in key columns
    customer360_df_cleaned = customer360_df.na.drop(subset=['customer_id', 'email'])
    return customer360_df_cleaned