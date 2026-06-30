import mlflow
import mlflow.spark

from mlflow.tracking import MlflowClient

from pyspark.sql.functions import (
    current_timestamp,
    lit
)

from pyspark.ml.evaluation import (
    RegressionEvaluator
)

------------------

CATALOG = "retailmart"

ML_SCHEMA = "ml_db"

MONITORING_TABLE = f"{CATALOG}.{ML_SCHEMA}.model_monitoring_results"

PREDICTION_TABLE = f"{CATALOG}.{ML_SCHEMA}.customer_spend_predictions"

-----------------------

mlflow.set_experiment(
    "/Shared/RetailMart_Model_Monitoring"
)

client = MlflowClient()

-------------------

prediction_df = spark.table(
    PREDICTION_TABLE
)

display(prediction_df)

print(
    "Prediction Records :",
    prediction_df.count()
)

------------------

mae_evaluator = RegressionEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="mae"
)

rmse_evaluator = RegressionEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="rmse"
)

r2_evaluator = RegressionEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="r2"
)

----------------

mae = mae_evaluator.evaluate(prediction_df)

rmse = rmse_evaluator.evaluate(prediction_df)

r2 = r2_evaluator.evaluate(prediction_df)

prediction_count = prediction_df.count()

------------------

print("=" * 60)

print("RetailMart Model Monitoring Metrics")

print("=" * 60)

print(f"Prediction Records : {prediction_count}")

print(f"MAE                : {mae:.2f}")

print(f"RMSE               : {rmse:.2f}")

print(f"R²                 : {r2:.4f}")

print("=" * 60)

------------------

RMSE_THRESHOLD = 60000

MAE_THRESHOLD = 50000

R2_THRESHOLD = -0.05

--------------------

if (
    rmse <= RMSE_THRESHOLD
    and
    mae <= MAE_THRESHOLD
    and
    r2 >= R2_THRESHOLD
):
    model_status = "Healthy"

elif (
    rmse <= RMSE_THRESHOLD * 1.10
):
    model_status = "Warning"

else:
    model_status = "Critical"

print("=" * 60)

print("Monitoring Status")

print("=" * 60)

print(f"Model Status : {model_status}")

print("=" * 60)

----------------------------------

with mlflow.start_run(run_name="Model Monitoring"):

    mlflow.set_tag(
        "project",
        "RetailMart"
    )

    mlflow.set_tag(
        "pipeline_stage",
        "Monitoring"
    )

    mlflow.set_tag(
        "registered_model",
        "customer_spend_prediction_rf"
    )

    mlflow.log_metric(
        "Prediction_Count",
        prediction_count
    )

    mlflow.log_metric(
        "MAE",
        mae
    )

    mlflow.log_metric(
        "RMSE",
        rmse
    )

    mlflow.log_metric(
        "R2",
        r2
    )

    mlflow.log_param(
        "Model_Status",
        model_status
    )

    monitoring_run_id = mlflow.active_run().info.run_id

print("Monitoring Run ID :", monitoring_run_id)

-------------------------

monitoring_df = spark.createDataFrame(
    [
        (
            prediction_count,
            mae,
            rmse,
            r2,
            model_status
        )
    ],
    schema=[
        "prediction_count",
        "mae",
        "rmse",
        "r2",
        "model_status"
    ]
).withColumn(
    "monitoring_timestamp",
    current_timestamp()
)

-------------------

monitoring_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(
        MONITORING_TABLE
    )

------------------

display(
    spark.table(
        MONITORING_TABLE
    ).orderBy(
        "monitoring_timestamp",
        ascending=False
    )
)

----------------

print("=" * 60)

print("RetailMart Model Monitoring Summary")

print("=" * 60)

print(f"Prediction Records : {prediction_count}")

print(f"MAE                : {mae:.2f}")

print(f"RMSE               : {rmse:.2f}")

print(f"R²                 : {r2:.4f}")

print(f"Model Status       : {model_status}")

print("=" * 60)

---------------------

latest_monitoring = (
    spark.table(MONITORING_TABLE)
    .orderBy("monitoring_timestamp", ascending=False)
    .limit(1)
)

display(latest_monitoring)

--------------------

print("=" * 60)

print("Notebook 07 Completed Successfully")

print("=" * 60)

print("✓ Batch Predictions Monitored")

print("✓ Performance Metrics Calculated")

print("✓ MLflow Monitoring Run Logged")

print("✓ Monitoring History Saved")

print("✓ Delta Monitoring Table Updated")

print("=" * 60)

--------------------

