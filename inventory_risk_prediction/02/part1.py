from pyspark.sql import functions as F

from pyspark.ml.feature import VectorAssembler

from pyspark.ml.classification import (
    LogisticRegression,
    DecisionTreeClassifier,
    RandomForestClassifier,
    GBTClassifier
)

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

import mlflow
import mlflow.spark

-----------------------

CATALOG = "retailmart"
SCHEMA = "ml_db"

FEATURE_TABLE = f"{CATALOG}.{SCHEMA}.inventory_risk_features"

MODEL_TABLE = f"{CATALOG}.{SCHEMA}.inventory_risk_model"

COMPARISON_TABLE = f"{CATALOG}.{SCHEMA}.inventory_model_comparison"

-------------------

feature_df = spark.table(FEATURE_TABLE)

display(feature_df.limit(10))

-------------------

feature_columns = [

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

]

----------------

assembler = VectorAssembler(

    inputCols=feature_columns,

    outputCol="features"

)

model_df = assembler.transform(feature_df)

---------------

model_df = model_df.select(

    "inventory_id",

    "product_id",

    "store_id",

    "inventory_risk",

    "label",

    "features"

)

display(model_df.limit(10))

----------------

train_df, test_df = model_df.randomSplit(

    [0.8, 0.2],

    seed=42

)

print("Training Rows :", train_df.count())

print("Testing Rows  :", test_df.count())

-------------------

evaluator = MulticlassClassificationEvaluator(

    labelCol="label",

    predictionCol="prediction"

)

------------------

