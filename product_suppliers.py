from pyspark.sql.functions import *
from pyspark.sql.types import *

silver_product_suppliers = (

    bronze_product_suppliers

    # Remove exact duplicates
    .dropDuplicates()

    # Type casting
    .withColumn(
        "product_id",
        col("product_id").cast(IntegerType())
    )

    .withColumn(
        "supplier_id",
        col("supplier_id").cast(IntegerType())
    )

    # Mandatory columns
    .filter(
        ~(
            col("product_id").isNull()
            |
            col("supplier_id").isNull()
        )
    )

    # Business rules
    .filter(
        col("product_id") > 0
    )

    .filter(
        col("supplier_id") > 0
    )

)