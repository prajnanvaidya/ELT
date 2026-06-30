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