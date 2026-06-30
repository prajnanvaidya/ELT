1.--------------------
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

2.----------------

from mlflow.models.signature import infer_signature

3.----------------

import mlflow

from pyspark.ml.regression import RandomForestRegressor

from pyspark.ml.tuning import (
    ParamGridBuilder,
    CrossValidator
)

from pyspark.ml.evaluation import RegressionEvaluator

from pyspark.sql import functions as F


4.----------------


CATALOG = "retailmart"

ML_SCHEMA = "ml_db"

MLFLOW_TMP_DIR = "/Volumes/retailmart/ml_db/mlflow_artifacts"

import osos.environ["SPARKML_TEMP_DFS_PATH"] = MLFLOW_TMP_DIR

5.----------------


train_df = spark.table(
    f"{CATALOG}.{ML_SCHEMA}.customer_spend_train"
)

6.----------------


test_df = spark.table(
    f"{CATALOG}.{ML_SCHEMA}.customer_spend_test"
)

7.----------------

display(train_df)

display(test_df)

8.----------------


print("Training Records :", train_df.count())

print("Testing Records :", test_df.count())

9.----------------

baseline_model_df = spark.table(
    f"{CATALOG}.{ML_SCHEMA}.best_customer_spend_model"
)

10.----------------

display(
    baseline_model_df
)

11.----------------

baseline = baseline_model_df.first()

baseline_rmse = baseline["RMSE"]

baseline_mae = baseline["MAE"]

baseline_r2 = baseline["R2"]

12.----------------

print("=" * 60)

print("Baseline Model Performance")

print("=" * 60)

print(f"RMSE : {baseline_rmse:.4f}")

print(f"MAE  : {baseline_mae:.4f}")

print(f"R²   : {baseline_r2:.4f}")

print("=" * 60)

13.----------------

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

14.----------------


rf = RandomForestRegressor(

    featuresCol="features",

    labelCol="label",

    predictionCol="prediction",

    seed=42

)

15.----------------

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

16.----------------

print(

    "Total Hyperparameter Combinations :",

    len(param_grid)

)

17.----------------


cross_validator = CrossValidator(

    estimator=rf,

    estimatorParamMaps=param_grid,

    evaluator=rmse_evaluator,

    numFolds=3,

    seed=42

)


