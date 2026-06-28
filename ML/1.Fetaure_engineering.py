Cell 1 (Markdown)
# RetailMart ML

## Notebook 01 - Feature Engineering

### Objective

Create an ML feature table for Customer Spend Prediction using RetailMart Silver tables.

The notebook:

- Loads Silver tables
- Uses only completed orders
- Splits each customer's order history chronologically
- Creates historical features
- Creates future spending target
- Saves the final ML feature table
Cell 2
from pyspark.sql import functions as F
from pyspark.sql.window import Window
Cell 3
CATALOG = "retailmart"

SILVER_SCHEMA = "silver_db"

ML_SCHEMA = "ml_db"
Cell 4
customers_df = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}.silver_customers"
)

orders_df = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}.silver_orders"
)
Cell 5
display(customers_df)

display(orders_df)
Cell 6

Only completed orders contribute to customer spending.

completed_orders_df = (
    orders_df
    .filter(F.col("order_status") == "Completed")
)
Cell 7

Assign chronological order number for every customer.

customer_window = Window.partitionBy("customer_id").orderBy("order_date")

completed_orders_df = (
    completed_orders_df
    .withColumn(
        "order_sequence",
        F.row_number().over(customer_window)
    )
)
Cell 8

Calculate total completed orders for every customer.

customer_count_window = Window.partitionBy("customer_id")

completed_orders_df = (
    completed_orders_df
    .withColumn(
        "total_completed_orders",
        F.count("*").over(customer_count_window)
    )
)
Cell 9

Create historical cutoff (80%).

completed_orders_df = (
    completed_orders_df
    .withColumn(
        "historical_cutoff",
        F.floor(
            F.col("total_completed_orders") * 0.8
        )
    )
)
Cell 10

Historical Orders

historical_orders_df = (

    completed_orders_df

    .filter(
        F.col("order_sequence")
        <=
        F.col("historical_cutoff")
    )

)
Cell 11

Future Orders

future_orders_df = (

    completed_orders_df

    .filter(
        F.col("order_sequence")
        >
        F.col("historical_cutoff")
    )

)
Cell 12

Historical customer features.

historical_features_df = (

    historical_orders_df

    .groupBy("customer_id")

    .agg(

        F.count("order_id")
        .alias("historical_total_orders"),

        F.sum("total_amount")
        .alias("historical_total_spend"),

        F.avg("total_amount")
        .alias("historical_avg_order_value"),

        F.max("total_amount")
        .alias("historical_max_order"),

        F.min("total_amount")
        .alias("historical_min_order")

    )

)
Cell 13

Future spending target.

future_target_df = (

    future_orders_df

    .groupBy("customer_id")

    .agg(

        F.sum("total_amount")
        .alias("future_customer_spend")

    )

)
Cell 14

Customer tenure.

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
Cell 15

Preferred payment method from historical orders only.

payment_window = (

    Window

    .partitionBy("customer_id")

    .orderBy(F.desc("count"))

)

preferred_payment_df = (

    historical_orders_df

    .groupBy(
        "customer_id",
        "payment_method"
    )

    .count()

    .withColumn(
        "rank",
        F.row_number().over(payment_window)
    )

    .filter(F.col("rank") == 1)

    .select(

        "customer_id",

        F.col("payment_method")
        .alias("preferred_payment_method")

    )

)
Cell 16

Preferred sales channel from historical orders.

channel_window = (

    Window

    .partitionBy("customer_id")

    .orderBy(F.desc("count"))

)

preferred_channel_df = (

    historical_orders_df

    .groupBy(
        "customer_id",
        "sales_channel"
    )

    .count()

    .withColumn(
        "rank",
        F.row_number().over(channel_window)
    )

    .filter(F.col("rank") == 1)

    .select(

        "customer_id",

        F.col("sales_channel")
        .alias("preferred_sales_channel")

    )

)
Cell 17

Create final ML dataset.

customer_spend_ml_features = (

    customers_feature_df

    .join(

        historical_features_df,

        "customer_id",

        "inner"

    )

    .join(

        preferred_payment_df,

        "customer_id",

        "left"

    )

    .join(

        preferred_channel_df,

        "customer_id",

        "left"

    )

    .join(

        future_target_df,

        "customer_id",

        "inner"

    )

)
Cell 18

Preview dataset.

display(customer_spend_ml_features)
Cell 19

Save Delta table.

(

    customer_spend_ml_features

    .write

    .format("delta")

    .mode("overwrite")

    .saveAsTable(

        f"{CATALOG}.{ML_SCHEMA}.customer_spend_ml_features"

    )

)
Cell 20

Verify.

display(

    spark.table(

        f"{CATALOG}.{ML_SCHEMA}.customer_spend_ml_features"

    )

)
Final Output Schema

Your final Delta table will contain approximately these columns:

Column
customer_id
customer_name
email
phone
city
state
registration_date
loyalty_level
customer_tenure_days
historical_total_orders
historical_total_spend
historical_avg_order_value
historical_max_order
historical_min_order
preferred_payment_method
preferred_sales_channel
future_customer_spend