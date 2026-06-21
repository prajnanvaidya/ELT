from pyspark.sql.functions import *
from pyspark.sql.types import *

silver_purchase_order_items = (

    bronze_purchase_order_items

    # Remove exact duplicates
    .dropDuplicates()

    # Type casting
    .withColumn(
        "purchase_order_item_id",
        col("purchase_order_item_id").cast(IntegerType())
    )

    .withColumn(
        "purchase_order_id",
        col("purchase_order_id").cast(IntegerType())
    )

    .withColumn(
        "product_id",
        col("product_id").cast(IntegerType())
    )

    .withColumn(
        "quantity",
        col("quantity").cast(IntegerType())
    )

    .withColumn(
        "unit_cost",
        round(
            col("unit_cost").cast(DoubleType()),
            2
        )
    )

    # Mandatory fields
    .filter(
        ~(
            col("purchase_order_item_id").isNull()
            |
            col("purchase_order_id").isNull()
            |
            col("product_id").isNull()
            |
            col("quantity").isNull()
            |
            col("unit_cost").isNull()
        )
    )

    # Business Rules
    .filter(
        col("quantity") > 0
    )

    .filter(
        col("unit_cost") > 0
    )

)

silver_purchase_order_items.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "retailmart.silver.silver_purchase_order_items"
    )