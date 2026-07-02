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

city_indexer = StringIndexer(

    inputCol="city",

    outputCol="city_index",

    handleInvalid="keep"

)

customer_df = city_indexer.fit(customer_df).transform(customer_df)

---------------------------

state_indexer = StringIndexer(

    inputCol="state",

    outputCol="state_index",

    handleInvalid="keep"

)

customer_df = state_indexer.fit(customer_df).transform(customer_df)

--------------------------------

display(customer_df)

-------------------------------

assembler = VectorAssembler(

    inputCols=[

        "total_orders",

        "total_spend",

        "purchase_frequency",

        "repeat_customer_count",

        "avg_customer_spend",

        "spend_per_order"

    ],

    outputCol="raw_features"

)

customer_df = assembler.transform(customer_df)

-------------------------

scaler = StandardScaler(

    inputCol="raw_features",

    outputCol="features",

    withStd=True,

    withMean=True

)

scaler_model = scaler.fit(customer_df)

customer_df = scaler_model.transform(customer_df)

---------------------------

customer_cluster_features = customer_df.select(

    "customer_id",

    "customer_name",

    "city",

    "state",

    "city_index",

    "state_index",

    "total_orders",

    "total_spend",

    "purchase_frequency",

    "repeat_customer_count",

    "avg_customer_spend",

    "spend_per_order",

    "features"

)

------------------------------

display(customer_cluster_features)

------------------------

customer_cluster_features.printSchema()

--------------------------