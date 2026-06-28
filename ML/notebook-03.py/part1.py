Cell 1 (Markdown)
# RetailMart ML

## Notebook 03 - Model Training & Comparison

### Objective

Train multiple regression models for Customer Spend Prediction,
evaluate their performance,
track every experiment using MLflow,
and compare all models to identify the best-performing algorithm.

Models:

- Linear Regression
- Decision Tree Regressor
- Random Forest Regressor
- Gradient Boosted Tree Regressor

Evaluation Metrics:

- RMSE
- MAE
- R² Score
Cell 2

Import Libraries

from pyspark.sql import functions as F

import mlflow
import mlflow.spark

from pyspark.ml.regression import (
    LinearRegression,
    DecisionTreeRegressor,
    RandomForestRegressor,
    GBTRegressor
)

from pyspark.ml.evaluation import RegressionEvaluator
Cell 3

Catalog and Schema

CATALOG = "retailmart"

ML_SCHEMA = "ml_db"
Cell 4

Load Training and Testing Tables

train_df = spark.table(
    f"{CATALOG}.{ML_SCHEMA}.customer_spend_train"
)

test_df = spark.table(
    f"{CATALOG}.{ML_SCHEMA}.customer_spend_test"
)
Cell 5

Verify Dataset

print("Training Records :", train_df.count())

print("Testing Records :", test_df.count())

display(train_df)

display(test_df)
Cell 6

Create Evaluators

We will reuse these throughout the notebook.

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
Cell 7

Enable MLflow

mlflow.set_experiment("/Shared/RetailMart_Customer_Spend_Prediction")

If the experiment does not exist, Databricks will create it automatically.

Cell 8

Create Empty Model Comparison List

Instead of displaying metrics one by one,
we'll store every model's result.

model_results = []
Cell 9
Linear Regression
lr = LinearRegression(

    featuresCol="features",

    labelCol="label",

    predictionCol="prediction"

)
Cell 10

Train Linear Regression

with mlflow.start_run(run_name="Linear Regression"):

    lr_model = lr.fit(train_df)

    lr_predictions = lr_model.transform(test_df)
Cell 11

Evaluate Linear Regression

    lr_rmse = rmse_evaluator.evaluate(lr_predictions)

    lr_mae = mae_evaluator.evaluate(lr_predictions)

    lr_r2 = r2_evaluator.evaluate(lr_predictions)
Cell 12

Log to MLflow

    mlflow.log_param(

        "Model",

        "Linear Regression"

    )

    mlflow.log_metric(

        "RMSE",

        lr_rmse

    )

    mlflow.log_metric(

        "MAE",

        lr_mae

    )

    mlflow.log_metric(

        "R2",

        lr_r2

    )

    mlflow.spark.log_model(

        lr_model,

        "model"

    )
Cell 13

Store Comparison Result

    model_results.append(

        (

            "Linear Regression",

            lr_rmse,

            lr_mae,

            lr_r2

        )

    )
Cell 14

Preview Predictions

display(

    lr_predictions.select(

        "customer_id",

        "label",

        "prediction"

    )

)


after cell3:
MLFLOW_TMP_DIR = "/Volumes/retailmart/ml_db/mlflow_artifacts"

mlflow.spark.log_model(
    spark_model=lr_model,
    artifact_path="model",
    dfs_tmpdir=MLFLOW_TMP_DIR
)