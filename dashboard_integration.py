# RetailMart ML

## Notebook 09 - Dashboard Integration

### Objective

Prepare dashboard-ready Delta tables
for Customer Spend Prediction.

This notebook creates:

• KPI Table

• Prediction Distribution

• Loyalty Level Summary

• Top Customers

• Model Performance Summary

---------------------------

from pyspark.sql import functions as F

-----------------------------

CATALOG = "retailmart"

ML_SCHEMA = "ml_db"

----------------------------

prediction_df = spark.table(

    f"{CATALOG}.{ML_SCHEMA}.customer_spend_predictions"

)

---------------------------

monitoring_df = spark.table(

    f"{CATALOG}.{ML_SCHEMA}.model_monitoring_results"

)

------------------