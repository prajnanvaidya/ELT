from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.sql.functions import *

-------------

CATALOG = "retailmart"
SCHEMA = "ml_db"

FEATURE_TABLE = f"{CATALOG}.{SCHEMA}.inventory_risk_features"

MODEL_PATH = (
    f"/Volumes/{CATALOG}/{SCHEMA}/"
    "mlflow_artifacts/inventory_risk_model"
)

OUTPUT_TABLE = (
    f"{CATALOG}.{SCHEMA}.inventory_batch_predictions"
)

----------------

feature_df = spark.table(FEATURE_TABLE)

print("Rows :", feature_df.count())

display(feature_df.limit(10))

-----------------

model = RandomForestClassificationModel.load(
    MODEL_PATH
)

print("Random Forest model loaded successfully.")

---------------------

predictions = model.transform(feature_df)

display(

    predictions.select(

        "inventory_id",

        "product_id",

        "store_id",

        "inventory_risk",

        "prediction",

        "probability"

    )

)

----------------------

from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(

    inputCols=[

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

        "state_index"

    ],

    outputCol="features"

)

feature_df = assembler.transform(feature_df)

-----------------------

predictions.select(

    "inventory_id",

    "product_id",

    "store_id",

    "inventory_risk",

    "prediction",

    "probability"

).write \

.format("delta") \

.mode("overwrite") \

.saveAsTable(OUTPUT_TABLE)

print("Batch predictions saved.")

---------------------------

display(

    spark.table(OUTPUT_TABLE)

)

---------------------

summary_df = (

    predictions

    .groupBy("prediction")

    .count()

    .orderBy("prediction")

)

display(summary_df)

-------------------------

final_predictions = (

    predictions

    .withColumn(

        "predicted_risk",

        when(col("prediction") == 0, "Low Risk")

        .when(col("prediction") == 1, "High Risk")

        .otherwise("Medium Risk")

    )

)

display(

    final_predictions.select(

        "inventory_id",

        "product_id",

        "store_id",

        "predicted_risk",

        "probability"

    )

)

-------------------------

final_predictions.select(

    "inventory_id",

    "product_id",

    "store_id",

    "predicted_risk",

    "probability"

).write \

.format("delta") \

.mode("overwrite") \

.saveAsTable(

    f"{CATALOG}.{SCHEMA}.inventory_final_predictions"

)

print("Final prediction table saved.")

-------------------------------

print("Batch Prediction Table")

display(

    spark.table(OUTPUT_TABLE)

)

print("")

print("Final Prediction Table")

display(

    spark.table(

        f"{CATALOG}.{SCHEMA}.inventory_final_predictions"

    )

)

---------------------------

