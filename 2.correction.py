46

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
        lit(repeat_customers.count())
    )
)




after 45

customer_order_counts = (
    completed_orders
    .groupBy("customer_id")
    .agg(
        count("order_id").alias("total_orders")
    )
)


customer_order_counts = (
    customer_order_counts

    .withColumn(
        "purchase_frequency",
        col("total_orders")
    )

    .withColumn(
        "repeat_customer_count",
        when(col("total_orders") > 1, 1).otherwise(0)
    )
)

46
gold_customer_purchase_summary = (

    gold_customer_purchase_summary.alias("g")

    .join(
        customer_order_counts
            .select(
                "customer_id",
                "purchase_frequency",
                "repeat_customer_count"
            )
            .alias("c"),

        "customer_id",

        "left"
    )

    .fillna({
        "purchase_frequency":0,
        "repeat_customer_count":0
    })
)


.withColumn(

    "avg_customer_spend",

    round(
        col("total_spend") /
        when(col("total_orders")==0,1)
        .otherwise(col("total_orders")),
        2
    )

)