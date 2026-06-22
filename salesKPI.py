03_gold_kpi_design
Cell 1 — Imports
from pyspark.sql.functions import *
from pyspark.sql.types import *
Cell 2 — Read Silver Tables
silver_orders = spark.table(
    "retailmart.silver.silver_orders"
)

silver_stores = spark.table(
    "retailmart.silver.silver_stores"
)
Cell 3 — Verify Counts
print("Orders :", silver_orders.count())
print("Stores :", silver_stores.count())
SALES KPI 1
gold_daily_sales

Business Objective:

Daily Revenue
Daily Orders
Average Order Value
Cell 4
gold_daily_sales = (

    silver_orders

    .groupBy("order_date")

    .agg(

        round(
            sum("total_amount"),
            2
        ).alias("total_revenue"),

        count("order_id")
        .alias("total_orders"),

        round(
            avg("total_amount"),
            2
        ).alias("avg_order_value")

    )

    .orderBy("order_date")
)
Cell 5
display(gold_daily_sales)
Cell 6
gold_daily_sales.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable(
    "retailmart.gold.gold_daily_sales"
)
SALES KPI 2
gold_monthly_sales

Business Objective:

Monthly revenue trend

Cell 7
gold_monthly_sales = (

    silver_orders

    .withColumn(
        "year",
        year("order_date")
    )

    .withColumn(
        "month",
        month("order_date")
    )

    .groupBy(
        "year",
        "month"
    )

    .agg(

        round(
            sum("total_amount"),
            2
        ).alias("monthly_revenue"),

        count("order_id")
        .alias("monthly_orders")

    )

    .orderBy(
        "year",
        "month"
    )
)
Cell 8
display(gold_monthly_sales)
Cell 9
gold_monthly_sales.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable(
    "retailmart.gold.gold_monthly_sales"
)
SALES KPI 3
gold_store_sales

Business Objective:

Store-wise sales performance

Cell 10
gold_store_sales = (

    silver_orders.alias("o")

    .join(
        silver_stores.alias("s"),
        "store_id"
    )

    .groupBy(
        "store_id",
        "store_name",
        "city",
        "state"
    )

    .agg(

        round(
            sum("total_amount"),
            2
        ).alias("store_revenue"),

        count("order_id")
        .alias("total_orders"),

        round(
            avg("total_amount"),
            2
        ).alias("avg_order_value")

    )

    .orderBy(
        desc("store_revenue")
    )
)
Cell 11
display(gold_store_sales)
Cell 12
gold_store_sales.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable(
    "retailmart.gold.gold_store_sales"
)
SALES KPI 4
gold_sales_channel_performance

Business Objective:

Compare Online vs Store sales

Cell 13
gold_sales_channel_performance = (

    silver_orders

    .groupBy(
        "sales_channel"
    )

    .agg(

        round(
            sum("total_amount"),
            2
        ).alias("channel_revenue"),

        count("order_id")
        .alias("total_orders"),

        round(
            avg("total_amount"),
            2
        ).alias("avg_order_value")

    )

    .orderBy(
        desc("channel_revenue")
    )
)
Cell 14
display(
    gold_sales_channel_performance
)
Cell 15
gold_sales_channel_performance.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable(
    "retailmart.gold.gold_sales_channel_performance"
)
SALES KPI SUMMARY TABLE

This is useful for dashboard cards:

Total Revenue
Total Orders
Avg Order Value
Cell 16
gold_sales_summary = (

    silver_orders

    .agg(

        round(
            sum("total_amount"),
            2
        ).alias("total_revenue"),

        count("order_id")
        .alias("total_orders"),

        round(
            avg("total_amount"),
            2
        ).alias("avg_order_value")

    )
)
Cell 17
display(gold_sales_summary)
Cell 18
gold_sales_summary.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable(
    "retailmart.gold.gold_sales_summary"
)
Final Validation Cell
spark.sql("""
SHOW TABLES IN retailmart.gold
""").show(truncate=False)

After this, your Sales KPI section (10.1 in SDD) will be fully implemented and you'll have:

gold_daily_sales
gold_monthly_sales
gold_store_sales
gold_sales_channel_performance
gold_sales_summary