# ============================================
# NOTEBOOK 01
# CUSTOMER CLUSTERING - FEATURE ENGINEERING
# ============================================

------------------------

from pyspark.sql import functions as F

from pyspark.sql.functions import (
    col,
    when,
    lit,
    round,
    greatest
)

from pyspark.ml.feature import (
    StringIndexer,
    VectorAssembler,
    StandardScaler
)

----------------------

DATABASE = "retailmart.ml_db"

SOURCE_TABLE = "retailmart.gold.gold_customer_purchase_summary"

FEATURE_TABLE = f"{DATABASE}.customer_cluster_features"

---------------------------

customer_df = spark.table(SOURCE_TABLE)

---------------------

print("Source Table Schema")

customer_df.printSchema()

print()

print("Total Customers")

print(customer_df.count())

-------------------------

display(customer_df)

------------------

customer_df = customer_df.select(

    "customer_id",

    "customer_name",

    "city",

    "state",

    "total_orders",

    "total_spend",

    "purchase_frequency",

    "repeat_customer_count",

    "avg_customer_spend"

)

-------------------------

customer_df = customer_df.fillna({

    "city": "Unknown",

    "state": "Unknown",

    "total_orders": 0,

    "total_spend": 0,

    "purchase_frequency": 0,

    "repeat_customer_count": 0,

    "avg_customer_spend": 0

})

----------------------

customer_df = customer_df.withColumn(

    "spend_per_order",

    round(

        col("total_spend") /

        greatest(col("total_orders"), lit(1)),

        2

    )

)

------------------------

display(customer_df)

----------------------

customer_df.select(

    [

        F.count(

            when(col(c).isNull(), c)

        ).alias(c)

        for c in customer_df.columns

    ]

).display()

-----------------------------

customer_df.printSchema()

-------------------

