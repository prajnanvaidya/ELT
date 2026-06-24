gold_order_status_performance = (

    silver_orders

    .groupBy("order_status")

    .agg(

        count("order_id")
        .alias("total_orders"),

        round(
            sum("total_amount"),
            2
        ).alias("revenue")

    )

)



sales_orders = silver_orders.filter(
    col("order_status") == "Completed"
)



gold_store_monthly_sales = (

    sales_orders.alias("o")

    .join(
        silver_stores.alias("s"),
        "store_id"
    )

    .withColumn(
        "year",
        year("order_date")
    )

    .withColumn(
        "month",
        month("order_date")
    )

    .groupBy(
        "store_id",
        "store_name",
        "year",
        "month"
    )

    .agg(

        round(
            sum("total_amount"),
            2
        ).alias("monthly_revenue"),

        count("order_id")
        .alias("monthly_orders"),

        round(
            avg("total_amount"),
            2
        ).alias("avg_order_value")

    )

    .orderBy(
        "store_id",
        "year",
        "month"
    )

)


gold_store_monthly_sales.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable(
    "retailmart.gold.gold_store_monthly_sales"
)








gold_state_sales = (

    sales_orders.alias("o")

    .join(
        silver_stores.alias("s"),
        "store_id"
    )

    .groupBy(
        "state"
    )

    .agg(

        round(
            sum("total_amount"),
            2
        ).alias("state_revenue"),

        count("order_id")
        .alias("total_orders"),

        round(
            avg("total_amount"),
            2
        ).alias("avg_order_value")

    )

    .orderBy(
        desc("state_revenue")
    )

)


gold_state_sales.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable(
    "retailmart.gold.gold_state_sales"
)


