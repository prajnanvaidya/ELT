from pyspark.sql.functions import *
from pyspark.sql.types import *

silver_orders = (

    bronze_orders

    # Remove exact duplicate rows
    .dropDuplicates()

    # -------------------------
    # Standardization
    # -------------------------

    .withColumn(
        "sales_channel",
        trim(initcap(col("sales_channel")))
    )

    .withColumn(
        "payment_method",
        trim(initcap(col("payment_method")))
    )

    .withColumn(
        "order_status",
        trim(initcap(col("order_status")))
    )

    # -------------------------
    # Explicit Type Casting
    # -------------------------

    .withColumn(
        "order_id",
        col("order_id").cast(IntegerType())
    )

    .withColumn(
        "customer_id",
        col("customer_id").cast(IntegerType())
    )

    .withColumn(
        "store_id",
        col("store_id").cast(IntegerType())
    )

    .withColumn(
        "order_date",
        to_date(col("order_date"))
    )

    .withColumn(
        "total_amount",
        col("total_amount").cast(DoubleType())
    )

    # -------------------------
    # Mandatory Columns
    # -------------------------

    .filter(
        ~(
            col("order_id").isNull()
            |
            col("customer_id").isNull()
            |
            col("store_id").isNull()
            |
            col("order_date").isNull()
            |
            col("sales_channel").isNull()
            |
            col("payment_method").isNull()
            |
            col("order_status").isNull()
            |
            col("total_amount").isNull()
        )
    )

    # -------------------------
    # Business Rules
    # -------------------------

    .filter(
        col("total_amount") > 0
    )

    .filter(
        col("order_date") <= current_date()
    )

    .filter(
        col("sales_channel").isin(
            "Ecommerce",
            "In-store"
        )
    )

    .filter(
        col("payment_method").isin(
            "Cash",
            "Credit Card",
            "Debit Card",
            "Upi"
        )
    )

    .filter(
        col("order_status").isin(
            "Completed",
            "Pending",
            "Cancelled"
        )
    )

    # -------------------------
    # Retail Business Rule
    # -------------------------

    .withColumn(
        "order_status",

        when(

            (col("sales_channel") == "In-store")

            &

            col("order_status").isin(
                "Pending",
                "Cancelled"
            ),

            "Completed"

        )

        .otherwise(
            col("order_status")
        )

    )

)