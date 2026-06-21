from pyspark.sql.functions import *
from pyspark.sql.types import *

silver_purchase_orders = (

    bronze_purchase_orders

    # Remove exact duplicate rows
    .dropDuplicates()

    # -------------------------
    # Standardization
    # -------------------------

    .withColumn(
        "status",
        trim(initcap(col("status")))
    )

    # -------------------------
    # Explicit Type Casting
    # -------------------------

    .withColumn(
        "purchase_order_id",
        col("purchase_order_id").cast(IntegerType())
    )

    .withColumn(
        "supplier_id",
        col("supplier_id").cast(IntegerType())
    )

    .withColumn(
        "purchase_order_date",
        to_date(col("purchase_order_date"))
    )

    .withColumn(
        "expected_delivery_date",
        to_date(col("expected_delivery_date"))
    )

    .withColumn(
    "expected_delivery_date",

    when(
        (col("status") == "Created")
        &
        (col("expected_delivery_date") < current_date()),

        date_add(
            current_date(),
            floor(rand()*18 + 7).cast("int")
        )
    )

    .otherwise(
        col("expected_delivery_date")
    )
    )

    .withColumn(
    "status",
    when(
        (col("status") == "Delivered")
        &
        (col("expected_delivery_date") > current_date()),

        "Approved"
    )

    .otherwise(col("status"))
    )

    # -------------------------
    # Mandatory Columns
    # -------------------------

    .filter(
        ~(
            col("purchase_order_id").isNull()
            |
            col("supplier_id").isNull()
            |
            col("purchase_order_date").isNull()
            |
            col("expected_delivery_date").isNull()
            |
            col("status").isNull()
        )
    )

    # -------------------------
    # Business Rules
    # -------------------------

    .filter(
        col("purchase_order_id") > 0
    )

    .filter(
        col("supplier_id") > 0
    )

    .filter(
        col("purchase_order_date")
        <= current_date()
    )

    .filter(
        col("expected_delivery_date")
        >= col("purchase_order_date")
    )
    
    .filter(
    ~(
        (col("status") == "Created")
        &
        (col("expected_delivery_date") < current_date())
    )
    )
)