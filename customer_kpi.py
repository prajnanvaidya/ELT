I checked all the schemas carefully.

silver_orders
order_id
customer_id
store_id
order_date
sales_channel
payment_method
order_status
total_amount
silver_customers
customer_id
customer_name
email
phone
city
state
registration_date
loyalty_level
last_updated

Your SDD Customer KPIs are:

Total Customers
Repeat Customers
Purchase Frequency
Avg Customer Spend
Loyalty Distribution

And Gold Tables:

gold_customer_metrics
gold_customer_purchase_summary
03_gold_kpi_design
CUSTOMER KPI SECTION
Cell 19 — Load Tables
silver_customers = spark.table(
    "retailmart.silver.silver_customers"
)

silver_orders = spark.table(
    "retailmart.silver.silver_orders"
)
KPI 1
Total Customers
KPI 2
Loyalty Distribution

These belong in:

gold_customer_metrics
Cell 20
total_customers = silver_customers.count()
Cell 21
loyalty_distribution = (

    silver_customers

    .groupBy("loyalty_level")

    .agg(
        count("customer_id")
        .alias("customer_count")
    )
)
Cell 22
display(loyalty_distribution)
Cell 23
gold_customer_metrics = (

    loyalty_distribution

    .withColumn(
        "total_customers",
        lit(total_customers)
    )

    .select(
        "loyalty_level",
        "customer_count",
        "total_customers"
    )
)
Cell 24
display(gold_customer_metrics)
Cell 25
gold_customer_metrics.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable(
    "retailmart.gold.gold_customer_metrics"
)
KPI 3
Repeat Customers

Definition from SDD:

Customers having more than 1 order
Cell 26
repeat_customers = (

    silver_orders

    .groupBy("customer_id")

    .agg(
        count("order_id")
        .alias("order_count")
    )

    .filter(
        col("order_count") > 1
    )
)
Cell 27
display(repeat_customers)
KPI 4
Purchase Frequency

Formula:

Total Orders / Total Customers
Cell 28
total_orders = silver_orders.count()

total_customers = silver_customers.count()

purchase_frequency = round(
    total_orders / total_customers,
    2
)
KPI 5
Average Customer Spend

Formula:

Total Revenue / Total Customers
Cell 29
total_revenue = (

    silver_orders

    .agg(
        sum("total_amount")
    )

    .first()[0]
)
Cell 30
avg_customer_spend = round(
    total_revenue / total_customers,
    2
)
Customer Purchase Summary

SDD Table:

gold_customer_purchase_summary
Cell 31
gold_customer_purchase_summary = (

    silver_customers.alias("c")

    .join(
        silver_orders.alias("o"),
        "customer_id",
        "left"
    )

    .groupBy(

        "customer_id",
        "customer_name",
        "city",
        "state",
        "loyalty_level"

    )

    .agg(

        count("order_id")
        .alias("total_orders"),

        round(
            sum("total_amount"),
            2
        ).alias("total_spend")

    )

    .fillna(
        {
            "total_orders": 0,
            "total_spend": 0
        }
    )
)
Cell 32
display(
    gold_customer_purchase_summary
)
Add Customer KPIs
Cell 33
repeat_customer_count = (
    repeat_customers.count()
)
Cell 34
gold_customer_purchase_summary = (

    gold_customer_purchase_summary

    .withColumn(
        "purchase_frequency",
        lit(purchase_frequency)
    )

    .withColumn(
        "avg_customer_spend",
        lit(avg_customer_spend)
    )

    .withColumn(
        "repeat_customer_count",
        lit(repeat_customer_count)
    )
)
Cell 35
display(
    gold_customer_purchase_summary
)
Cell 36
gold_customer_purchase_summary.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable(
    "retailmart.gold.gold_customer_purchase_summary"
)
Customer Dashboard Summary Table (Recommended)

This wasn't explicitly in SDD but is extremely useful for Power BI KPI cards.

Cell 37
gold_customer_summary = (

    spark.createDataFrame(
        [
            (
                total_customers,
                repeat_customer_count,
                purchase_frequency,
                avg_customer_spend
            )
        ],
        [
            "total_customers",
            "repeat_customers",
            "purchase_frequency",
            "avg_customer_spend"
        ]
    )
)
Cell 38
display(gold_customer_summary)
Cell 39
gold_customer_summary.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable(
    "retailmart.gold.gold_customer_summary"
)
Final Validation
spark.sql("""
SHOW TABLES IN retailmart.gold
""").show(truncate=False)

After this your Customer KPI section (10.2) is fully implemented and you'll have:

gold_customer_metrics
gold_customer_purchase_summary
gold_customer_summary