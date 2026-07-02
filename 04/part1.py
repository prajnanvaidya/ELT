# Inventory Risk Dashboard Tables

This notebook prepares Gold dashboard tables for Inventory Risk Analytics.

It performs the following tasks:

1. Reads Inventory Risk Features
2. Reads Inventory Risk Predictions
3. Joins Features with Predictions
4. Creates Dashboard Detail Table
5. Creates Summary Tables
6. Saves Gold Dashboard Tables

-------------------------------

from pyspark.sql.functions import *

from pyspark.sql.types import *

from pyspark.ml.functions import vector_to_array

-----------------------------

CATALOG = "retailmart"

SCHEMA = "ml_db"

FEATURE_TABLE = f"{CATALOG}.{SCHEMA}.inventory_risk_features"

PREDICTION_TABLE = f"{CATALOG}.{SCHEMA}.inventory_risk_predictions"

DETAIL_TABLE = f"{CATALOG}.{SCHEMA}.inventory_dashboard_detail"

SUMMARY_TABLE = f"{CATALOG}.{SCHEMA}.inventory_dashboard_summary"

CATEGORY_TABLE = f"{CATALOG}.{SCHEMA}.inventory_category_summary"

STORE_TABLE = f"{CATALOG}.{SCHEMA}.inventory_store_summary"

RISK_TABLE = f"{CATALOG}.{SCHEMA}.inventory_risk_distribution"

-----------------------------

feature_df = spark.table(FEATURE_TABLE)

prediction_df = spark.table(PREDICTION_TABLE)

print("Feature Rows :", feature_df.count())

print("Prediction Rows :", prediction_df.count())

-------------------

display(feature_df.limit(5))

display(prediction_df.limit(5))

-------------------

prediction_df = (

    prediction_df

    .withColumn(

        "probability_array",

        vector_to_array(col("probability"))

    )

    .withColumn(

        "low_risk_probability",

        round(col("probability_array")[0], 4)

    )

    .withColumn(

        "high_risk_probability",

        round(col("probability_array")[1], 4)

    )

    .withColumn(

        "medium_risk_probability",

        round(col("probability_array")[2], 4)

    )

    .drop(

        "probability",

        "probability_array"

    )

)

-------------------------

dashboard_df = (

    feature_df.alias("f")

    .join(

        prediction_df.alias("p"),

        on=[

            "inventory_id",

            "product_id",

            "store_id"

        ],

        how="inner"

    )

)

------------------------

dashboard_df = dashboard_df.select(

    "inventory_id",

    "product_id",

    "store_id",

    "stock_quantity",

    "reorder_level",

    "safety_stock",

    "stock_gap",

    "stock_to_reorder_ratio",

    "inventory_buffer",

    "safety_stock_ratio",

    "days_since_update",

    "category_index",

    "brand_index",

    "city_index",

    "state_index",

    col("f.inventory_risk").alias("actual_risk"),

    "prediction",

    "low_risk_probability",

    "high_risk_probability",

    "medium_risk_probability"

)

--------------------------------

dashboard_df.write \

.format("delta") \

.mode("overwrite") \

.saveAsTable(DETAIL_TABLE)

print("Inventory Dashboard Detail Table Saved.")

---------------------------------

display(

    spark.table(DETAIL_TABLE)

)

--------------------------

