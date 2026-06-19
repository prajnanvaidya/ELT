from pyspark.sql.functions import *
from pyspark.sql.types import *

silver_order_items = (

    bronze_order_items

    # Remove exact duplicates
    .dropDuplicates()

    # Type Casting
    .withColumn(
        "order_item_id",
        col("order_item_id").cast(IntegerType())
    )

    .withColumn(
        "order_id",
        col("order_id").cast(IntegerType())
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
        "item_amount",
        col("item_amount").cast(DoubleType())
    )

    # Mandatory columns
    .filter(

        ~(
            col("order_item_id").isNull()
            |
            col("order_id").isNull()
            |
            col("product_id").isNull()
            |
            col("quantity").isNull()
            |
            col("item_amount").isNull()
        )

    )

    # Business Rules
    .filter(
        col("quantity") > 0
    )

    .filter(
        col("item_amount") > 0
    )

)



silver_order_items.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "retailmart.silver.silver_order_items"
    )


display(
    spark.table(
        "retailmart.silver.silver_order_items"
    )
)