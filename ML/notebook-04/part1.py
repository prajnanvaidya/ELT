# RetailMart ML

## Notebook 04 - Hyperparameter Tuning

### Objective

Optimize the best performing model (Random Forest Regressor)
using Hyperparameter Tuning.

This notebook performs:

• Load Train/Test datasets
• Define baseline Random Forest
• Create Hyperparameter Grid
• Perform Cross Validation
• Select Best Model
• Evaluate Tuned Model
• Compare with Baseline Model
• Track experiment using MLflow
• Save Tuned Model

----------------

import mlflow

from pyspark.ml.regression import RandomForestRegressor

from pyspark.ml.tuning import (
    ParamGridBuilder,
    CrossValidator
)

from pyspark.ml.evaluation import RegressionEvaluator

from pyspark.sql import functions as F


----------------


CATALOG = "retailmart"

ML_SCHEMA = "ml_db"

MLFLOW_TMP_DIR = "/Volumes/retailmart/ml_db/mlflow_artifacts"


----------------


train_df = spark.table(
    f"{CATALOG}.{ML_SCHEMA}.customer_spend_train"
)

----------------


test_df = spark.table(
    f"{CATALOG}.{ML_SCHEMA}.customer_spend_test"
)

----------------

display(train_df)

display(test_df)

----------------


print("Training Records :", train_df.count())

print("Testing Records :", test_df.count())

----------------

baseline_model_df = spark.table(
    f"{CATALOG}.{ML_SCHEMA}.best_customer_spend_model"
)

----------------

display(
    baseline_model_df
)

----------------

baseline = baseline_model_df.first()

baseline_rmse = baseline["RMSE"]

baseline_mae = baseline["MAE"]

baseline_r2 = baseline["R2"]

----------------

print("=" * 60)

print("Baseline Model Performance")

print("=" * 60)

print(f"RMSE : {baseline_rmse:.4f}")

print(f"MAE  : {baseline_mae:.4f}")

print(f"R²   : {baseline_r2:.4f}")

print("=" * 60)

----------------

rmse_evaluator = RegressionEvaluator(

    labelCol="label",

    predictionCol="prediction",

    metricName="rmse"

)

mae_evaluator = RegressionEvaluator(

    labelCol="label",

    predictionCol="prediction",

    metricName="mae"

)

r2_evaluator = RegressionEvaluator(

    labelCol="label",

    predictionCol="prediction",

    metricName="r2"

)

----------------


rf = RandomForestRegressor(

    featuresCol="features",

    labelCol="label",

    predictionCol="prediction",

    seed=42

)

----------------

param_grid = (

    ParamGridBuilder()

    .addGrid(

        rf.numTrees,

        [50, 100, 150]

    )

    .addGrid(

        rf.maxDepth,

        [5, 10, 15]

    )

    .addGrid(

        rf.maxBins,

        [32, 64]

    )

    .build()

)

----------------

print(

    "Total Hyperparameter Combinations :",

    len(param_grid)

)

----------------


cross_validator = CrossValidator(

    estimator=rf,

    estimatorParamMaps=param_grid,

    evaluator=rmse_evaluator,

    numFolds=3,

    seed=42

)

----------------

