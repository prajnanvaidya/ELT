# RetailMart ML

## Notebook 06 - Batch Inference

### Objective

Perform batch inference using the registered Random Forest model.

This notebook performs:

• Load Registered Model
• Load Test Dataset
• Generate Predictions
• Save Prediction Results
• Display Predictions
• Verify Batch Inference

------------------

import mlflow
import mlflow.spark

------------

CATALOG = "retailmart"

ML_SCHEMA = "ml_db"

MODEL_NAME = f"{CATALOG}.{ML_SCHEMA}.customer_spend_prediction_rf"

----------------

model_uri = f"models:/{MODEL_NAME}/1"

print(model_uri)

----------------

loaded_model = mlflow.spark.load_model(model_uri)\

---------------

test_df = spark.table(
    f"{CATALOG}.{ML_SCHEMA}.customer_spend_test"
)

---------------

display(test_df)

---------------

print("Testing Records :", test_df.count())

--------------

predictions = loaded_model.transform(test_df)

--------------

display(predictions)

--------------

from pyspark.sql.functions import current_timestamp

prediction_results = predictions.select(

    "customer_id",

    "label",

    "prediction"

).withColumn(

    "prediction_timestamp",

    current_timestamp()

)

--------------

display(prediction_results)

--------------

prediction_results.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(

        f"{CATALOG}.{ML_SCHEMA}.customer_spend_predictions"

    )

-----------------

display(

    spark.table(

        f"{CATALOG}.{ML_SCHEMA}.customer_spend_predictions"

    )

)

------------------

print("=" * 60)

print("Prediction Summary")

print("=" * 60)

print("Prediction Records :",

      prediction_results.count())

print("=" * 60)

-----------------

print("=" * 60)

print("Notebook 06 Completed Successfully")

print("=" * 60)

print("✓ Registered Model Loaded")

print("✓ Batch Inference Completed")

print("✓ Predictions Saved")

print("✓ Delta Table Created")

print("=" * 60)

-------------------

