# RetailMart ML

## Notebook 05 - Model Registry

### Objective

Register the tuned Random Forest model into the Unity Catalog
Model Registry so it can be reused for inference,
monitoring, and deployment.

This notebook performs:

- Load the tuned Random Forest model
- Register the model
- Assign model version
- Add model description
- Verify registration

-------

import mlflow
import mlflow.spark

from mlflow.tracking import MlflowClient

---------

CATALOG = "retailmart"

ML_SCHEMA = "ml_db"

MLFLOW_TMP_DIR = "/Volumes/retailmart/ml_db/mlflow_artifacts"

----------

mlflow.set_experiment(
    "/Shared/RetailMart_Customer_Spend_Prediction"
)

--------

mlflow.set_experiment(
    "/Shared/RetailMart_Customer_Spend_Prediction"
)

------

client = MlflowClient()

---------

MODEL_NAME = f"{CATALOG}.{ML_SCHEMA}.customer_spend_prediction_rf"

-------

experiment = mlflow.get_experiment_by_name(
    "/Shared/RetailMart_Customer_Spend_Prediction"
)

experiment_id = experiment.experiment_id

---------

runs = mlflow.search_runs(

    experiment_ids=[experiment_id],

    filter_string="tags.model_type = 'tuned_random_forest'",

    order_by=["start_time DESC"]

)

-----------

display(runs)

------

if len(runs) == 0:

    raise Exception(
        "No Tuned Random Forest model found in MLflow."
    )

latest_run_id = runs.iloc[0]["run_id"]

print("Run ID :", latest_run_id)

----------

model_uri = f"runs:/{latest_run_id}/model"

print(model_uri)

-------

registered_model = mlflow.register_model(

    model_uri=model_uri,

    name=MODEL_NAME

)

---------

import time

time.sleep(10)

----

latest_version = client.get_model_version(

    MODEL_NAME,

    registered_model.version

)


-----

print("=" * 60)

print("Model Registered Successfully")

print("=" * 60)

print(f"Model Name    : {MODEL_NAME}")

print(f"Version       : {latest_version.version}")

print(f"Current Stage : {latest_version.current_stage}")

print("=" * 60)


-----

client.update_registered_model(

    name=MODEL_NAME,

    description="""
RetailMart Customer Spend Prediction

Algorithm:
Random Forest Regressor

Framework:
Apache Spark ML

Tracking:
MLflow

Project:
RetailMart Analytics Platform
"""
)


----------


for model in client.search_registered_models():

    if model.name == MODEL_NAME:

        print(model)


--------

print("=" * 60)

print("Notebook 05 Completed Successfully")

print("=" * 60)

print("✓ Model Registered")

print("✓ Unity Catalog Registry Updated")

print("✓ Ready for Batch Inference")

print("=" * 60)


---------