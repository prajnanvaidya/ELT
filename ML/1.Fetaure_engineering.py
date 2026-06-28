# RetailMart ML

## Notebook 01 - Feature Engineering

### Objective

Create a reusable ML feature table for Customer Spend Prediction using RetailMart Silver layer data.
    

Cell 2 – Import Libraries
from pyspark.sql import functions as F
from pyspark.sql.window import Window
Why?
functions provides aggregation and transformation functions.
Window is imported now because we'll use it later to identify preferred payment methods and sales channels.
Cell 3 – Define Catalog and Schemas
CATALOG = "retailmart"

SILVER_SCHEMA = "silver_db"
ML_SCHEMA = "ml_db"
Cell 4 – Load Required Tables
customers_df = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}.silver_customers"
)

orders_df = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}.silver_orders"
)
Cell 5 – Preview Data
display(customers_df)

display(orders_df)

This is mainly for notebook readability and sanity checking.

Cell 6 – Keep Only Completed Orders

Only completed orders contribute to actual customer spending.

Cancelled or pending orders should not influence the target.

completed_orders_df = (
    orders_df
    .filter(F.col("order_status") == "Completed")
)
Cell 7 – Customer Transaction Aggregation

Aggregate completed orders at the customer level.

customer_orders_df = (
    completed_orders_df
    .groupBy("customer_id")
    .agg(
        F.count("order_id").alias("total_orders"),
        F.sum("total_amount").alias("total_spend"),
        F.avg("total_amount").alias("avg_order_value"),
        F.max("total_amount").alias("max_order_value"),
        F.min("total_amount").alias("min_order_value")
    )
)

This creates the numerical spending features that summarize each customer's order history.

Cell 8 – Customer Tenure

Calculate how long each customer has been registered.

customers_feature_df = (
    customers_df
    .withColumn(
        "customer_tenure_days",
        F.datediff(
            F.current_date(),
            F.col("registration_date")
        )
    )
)

Customer tenure often correlates with purchasing behavior.

Cell 9 – Preferred Payment Method

Find the payment method used most frequently by each customer.

payment_window = Window.partitionBy("customer_id").orderBy(F.desc("count"))

preferred_payment_df = (
    completed_orders_df
    .groupBy("customer_id", "payment_method")
    .count()
    .withColumn(
        "rank",
        F.row_number().over(payment_window)
    )
    .filter(F.col("rank") == 1)
    .select(
        "customer_id",
        F.col("payment_method").alias("preferred_payment_method")
    )
)
Cell 10 – Preferred Sales Channel
channel_window = Window.partitionBy("customer_id").orderBy(F.desc("count"))

preferred_channel_df = (
    completed_orders_df
    .groupBy("customer_id", "sales_channel")
    .count()
    .withColumn(
        "rank",
        F.row_number().over(channel_window)
    )
    .filter(F.col("rank") == 1)
    .select(
        "customer_id",
        F.col("sales_channel").alias("preferred_sales_channel")
    )
)
Cell 11 – Create ML Feature Dataset

Join all engineered features into a single customer-level dataset.

customer_features_df = (
    customers_feature_df
    .join(customer_orders_df, "customer_id", "inner")
    .join(preferred_payment_df, "customer_id", "left")
    .join(preferred_channel_df, "customer_id", "left")
)
Cell 12 – Preview Feature Table
display(customer_features_df)

At this point, your dataset should contain columns such as:

customer_id
customer_name
city
state
loyalty_level
customer_tenure_days
total_orders
total_spend
avg_order_value
max_order_value
min_order_value
preferred_payment_method
preferred_sales_channel
Cell 13 – Save as Delta Table
(
    customer_features_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        f"{CATALOG}.{ML_SCHEMA}.customer_features_ml"
    )
)
Cell 14 – Verify Table Creation
display(
    spark.table(
        f"{CATALOG}.{ML_SCHEMA}.customer_features_ml"
    )
)