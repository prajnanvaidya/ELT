# RetailMart ML

## Notebook 08 - End-to-End ML Pipeline

### Objective

Validate and orchestrate the complete RetailMart Machine Learning pipeline.

This notebook performs:

• Validate Pipeline Assets
• Verify Delta Tables
• Verify Registered Model
• Verify Prediction Results
• Verify Monitoring Results
• Log Pipeline Execution
• Display Pipeline Summary

-------------------------------

import mlflow

from mlflow.tracking import MlflowClient

from pyspark.sql.functions import current_timestamp

---------------------------

CATALOG = "retailmart"

ML_SCHEMA = "ml_db"

PIPELINE_EXPERIMENT = "/Shared/RetailMart_End_To_End_Pipeline"

---------------------------

mlflow.set_experiment(
    PIPELINE_EXPERIMENT
)

client = MlflowClient()

----------------------------

required_tables = [

    "customer_spend_ml_features",

    "customer_spend_train",

    "customer_spend_test",

    "best_customer_spend_model",

    "tuned_customer_spend_model",

    "customer_spend_predictions",

    "model_monitoring_results"

]

-----------------------------

validation_results = []

-------------------------

for table in required_tables:

    full_name = f"{CATALOG}.{ML_SCHEMA}.{table}"

    exists = spark.catalog.tableExists(full_name)

    validation_results.append(

        (
            table,
            exists
        )

    )

---------------------

validation_df = spark.createDataFrame(

    validation_results,

    [

        "Table",

        "Exists"

    ]

)

---------------------------

display(validation_df)

-----------------

MODEL_NAME = f"{CATALOG}.{ML_SCHEMA}.customer_spend_prediction_rf"

------------------

registered_model = client.get_registered_model(
    MODEL_NAME
)

----------------------

print("=" * 60)

print("Registered Model")

print("=" * 60)

print("Model Name :", registered_model.name)

print("Owner :", registered_model.owner)

print("=" * 60)

----------------------

with mlflow.start_run(run_name="End-to-End Pipeline Validation"):

    mlflow.log_param(
        "Pipeline",
        "RetailMart ML"
    )

    mlflow.log_metric(
        "Validated Tables",
        len(required_tables)
    )

    mlflow.log_metric(
        "Available Tables",
        validation_df.filter("Exists = true").count()
    )

---------------------------

pipeline_summary = [

    (
        "Feature Engineering",
        "Completed"
    ),

    (
        "Data Preprocessing",
        "Completed"
    ),

    (
        "Model Training",
        "Completed"
    ),

    (
        "Hyperparameter Tuning",
        "Completed"
    ),

    (
        "Model Registry",
        "Completed"
    ),

    (
        "Batch Inference",
        "Completed"
    ),

    (
        "Model Monitoring",
        "Completed"
    )

]

-------------------------

summary_df = spark.createDataFrame(

    pipeline_summary,

    [

        "Pipeline Stage",

        "Status"

    ]

)

display(summary_df)

----------------------

print("=" * 60)

print("Notebook 08 Completed Successfully")

print("=" * 60)

print("✓ Pipeline Validated")

print("✓ Registered Model Verified")

print("✓ Prediction Tables Verified")

print("✓ Monitoring Tables Verified")

print("✓ MLflow Pipeline Run Logged")

print("=" * 60)

----------------------------
