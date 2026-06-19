from pyspark.sql.functions import *
from pyspark.sql.types import *

silver_suppliers = (

    bronze_suppliers

    # Remove exact duplicates
    .dropDuplicates()

    # -------------------------
    # Standardization
    # -------------------------

    .withColumn(
        "supplier_name",
        trim(col("supplier_name"))
    )

    .withColumn(
        "contact_person",
        trim(initcap(col("contact_person")))
    )

    .withColumn(
        "email",
        trim(lower(col("email")))
    )

    .withColumn(
        "city",
        trim(initcap(col("city")))
    )

    .withColumn(
        "country",
        trim(upper(col("country")))
    )

    # -------------------------
    # Phone Standardization
    # -------------------------

    .withColumn(
        "phone",
        regexp_replace(
            col("phone"),
            "[^0-9]",
            ""
        )
    )

    .withColumn(
        "phone",
        when(
            length(col("phone")) >= 10,
            substring(
                col("phone"),
                length(col("phone")) - 9,
                10
            )
        )
    )

    # -------------------------
    # Type Casting
    # -------------------------

    .withColumn(
        "supplier_id",
        col("supplier_id").cast(IntegerType())
    )

    .withColumn(
        "supplier_rating",
        col("supplier_rating").cast(DoubleType())
    )

    .withColumn(
        "last_updated",
        to_date(col("last_updated"))
    )

    # -------------------------
    # Mandatory Fields
    # -------------------------

    .filter(

        ~(
            col("supplier_id").isNull()
            |
            col("supplier_name").isNull()
            |
            col("contact_person").isNull()
            |
            col("phone").isNull()
            |
            col("email").isNull()
            |
            col("city").isNull()
            |
            col("country").isNull()
            |
            col("supplier_rating").isNull()
            |
            col("last_updated").isNull()
        )

    )

    # -------------------------
    # Business Rules
    # -------------------------

    .filter(
        (col("supplier_rating") >= 0)
        &
        (col("supplier_rating") <= 5)
    )

    .filter(
        col("last_updated") <= current_date()
    )

    .filter(
        col("country") == "USA"
    )

    .filter(
        length(col("phone")) == 10
    )

)


silver_suppliers.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "retailmart.silver.silver_suppliers"
    )