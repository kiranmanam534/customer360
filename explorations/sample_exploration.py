# Databricks notebook source
# MAGIC %sql
# MAGIC drop table customer360_dp.bronze.customers;
# MAGIC drop table customer360_dp.bronze.locations;
# MAGIC drop table customer360_dp.silver.customers_clean;
# MAGIC drop table customer360_dp.silver.locations_scd2

# COMMAND ----------

# !!! Before performing any data analysis, make sure to run the pipeline to materialize the sample datasets. The tables referenced in this notebook depend on that step.

display(spark.sql("SELECT * FROM customer360_dp.bronze.locations"))

# COMMAND ----------

display(spark.sql("SELECT * FROM customer360_dp.silver.locations_scd2"))

# COMMAND ----------

display(spark.sql("SELECT * FROM customer360_dp.silver.customers_clean"))

# COMMAND ----------

display(spark.sql("SELECT * FROM customer360_dp.gold.customer360"))

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_360 = spark.table("customer360_dp.gold.customer360")

display(df_360)



# COMMAND ----------

# DBTITLE 1,Executive KPI Table
df_kpi = df_360.agg(
    count("customer_id").alias("Total_orders"),
    sum("order_amount").alias("Total_Amount"),
    sum("payment_amount").alias("Total_Payment"),
    sum(when(col("payment_status")=="Success",1).otherwise(0)).alias("Total_success_payments"),
    sum(when(col("payment_status")=="Failed",1).otherwise(0)).alias("Total_failed_payments"),
    countDistinct("device_type").alias("Total_Devices"),
    countDistinct("city").alias("Total_Cities"),
    countDistinct("state").alias("Total_States"),
    countDistinct("country").alias("Total_Countries"),
    countDistinct("zipcode").alias("Total_Zipcodes"),
    countDistinct("ticket_id").alias("Total_Tickets"),
    countDistinct("issue_type").alias("Total_Issues"),
    countDistinct("session_id").alias("Total_Sessions"),
    countDistinct("page_viewed").alias("Total_Pages"),
    countDistinct("device_type").alias("Total_Devices")
)

display(df_kpi)

# COMMAND ----------

display(df_360.groupBy("customer_id", "name", "email", "gender", "city", "state", "country")
          .agg(
              countDistinct("order_id").alias("total_orders"),
              sum("order_amount").alias("lifetime_value"),
              max("order_date").alias("last_order_date"),
              max("session_time").alias("last_session_time"),
              countDistinct("ticket_id").alias("ticket_count")
          ))

# COMMAND ----------

# DBTITLE 1,Total payments done by payment method
# MAGIC %sql
# MAGIC select payment_method, count(*) as Total, sum(payment_amount) as Total_Amount from customer360_dp.gold.customer360 group by payment_method order by payment_method

# COMMAND ----------

# DBTITLE 1,total payments by device type
# MAGIC %sql
# MAGIC select device_type, count(*) as Total, count(order_id) as Total_orders, sum(payment_amount) as Total_Amount from customer360_dp.gold.customer360 group by device_type order by device_type

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Month wise Revenue
# MAGIC %sql
# MAGIC select date_format(order_date, "MMMM") as month_name, day(order_date) as day, sum(order_amount) as total from customer360_dp.gold.customer360 group by month_name, day order by month_name

# COMMAND ----------

# MAGIC %sql
# MAGIC select gender, count(*) as Gender_wise_Total from customer360_dp.gold.customer360 GROUP BY gender

# COMMAND ----------

# MAGIC %sql
# MAGIC select state, count(*) as  State_wise_Total from customer360_dp.silver.customers_clean group by state

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer360_dp.gold.customer_360_profile

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer360_dp.gold.fraud_analytics

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer360_dp.gold.customer360

# COMMAND ----------



# COMMAND ----------

df1 = df_360.groupBy("state").agg(count("customer_id").alias("total_customers"))
display(df1)

# COMMAND ----------

display(df_360.groupBy('order_status').agg(count("order_id").alias('total_orders'),sum("order_amount").alias("total_sales")))

# COMMAND ----------

display(df_360.groupBy(year(col('dob')),month(col('dob'))).agg(sum('order_amount').alias('total')))

# COMMAND ----------

display(df_360.groupBy('issue_type',year(col('ticket_date')).alias('Year'),date_format(col('ticket_date'),"MMMM").alias('Month')).agg(count("ticket_id").alias('total_tickets')))

# COMMAND ----------

display(df_360)

# COMMAND ----------


