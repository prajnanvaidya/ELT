bronze_inventory.filter(
    (col("safety_stock") > col("reorder_level"))
    |
    (col("reorder_level") > col("stock_quantity"))
).count()

print(
    bronze_inventory.count()
    -
    bronze_inventory.dropDuplicates().count()
)

from pyspark.sql.functions import *
from pyspark.sql.types import *

silver_inventory = (

    bronze_inventory

    # Remove exact duplicate rows
    .dropDuplicates()

    # -------------------------
    # Explicit Type Casting
    # -------------------------

    .withColumn(
        "inventory_id",
        col("inventory_id").cast(IntegerType())
    )

    .withColumn(
        "product_id",
        col("product_id").cast(IntegerType())
    )

    .withColumn(
        "store_id",
        col("store_id").cast(IntegerType())
    )

    .withColumn(
        "stock_quantity",
        col("stock_quantity").cast(IntegerType())
    )

    .withColumn(
        "reorder_level",
        col("reorder_level").cast(IntegerType())
    )

    .withColumn(
        "safety_stock",
        col("safety_stock").cast(IntegerType())
    )

    .withColumn(
        "last_updated",
        to_date(col("last_updated"))
    )

    # -------------------------
    # Mandatory Columns
    # -------------------------

    .filter(
        ~(
            col("inventory_id").isNull()
            |
            col("product_id").isNull()
            |
            col("store_id").isNull()
            |
            col("stock_quantity").isNull()
            |
            col("reorder_level").isNull()
            |
            col("safety_stock").isNull()
            |
            col("last_updated").isNull()
        )
    )

    # -------------------------
    # Business Rules
    # -------------------------

    .filter(
        col("stock_quantity") >= 0
    )

    .filter(
        col("reorder_level") >= 0
    )

    .filter(
        col("safety_stock") >= 0
    )

    .filter(
        col("last_updated") <= current_date()
    )

    .filter(
        col("safety_stock")
        <=
        col("reorder_level")
    )

    .filter(
        col("reorder_level")
        <=
        col("stock_quantity")
    )

)