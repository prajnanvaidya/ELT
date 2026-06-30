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

