from pyspark.sql import functions as F

import pandas as pd

import mlflow

from sklearn.linear_model import LinearRegression

----------------------

CATALOG = "retailmart"

ML_SCHEMA = "ml_db"

FEATURE_TABLE = f"{CATALOG}.{ML_SCHEMA}.sales_forecast_features"

PREDICTION_TABLE = f"{CATALOG}.{ML_SCHEMA}.sales_forecast_predictions"

-----------------

sales_df = spark.table(

    FEATURE_TABLE

)

display(sales_df)

-------------------

sales_pd = (

    sales_df

    .orderBy("forecast_date")

    .toPandas()

)

-----------------------

feature_columns = [

    "year",

    "month",

    "monthly_orders",

    "lag_1_revenue",

    "lag_2_revenue",

    "lag_3_revenue",

    "lag_1_orders",

    "rolling_avg_revenue",

    "revenue_growth_pct",

    "quarter"

]

target_column = "monthly_revenue"

----------------------------

model = LinearRegression()

model.fit(

    sales_pd[feature_columns],

    sales_pd[target_column]

)

------------------------

sales_pd["predicted_revenue"] = model.predict(

    sales_pd[feature_columns]

)

--------------------------

sales_pd["forecast_error"] = (

    sales_pd["monthly_revenue"]

    -

    sales_pd["predicted_revenue"]

)

sales_pd["absolute_error"] = (

    sales_pd["forecast_error"]

    .abs()

)

sales_pd["error_percent"] = (

    sales_pd["absolute_error"]

    /

    sales_pd["monthly_revenue"]

) * 100

--------------------------

prediction_df = sales_pd[

    [

        "forecast_date",

        "year",

        "month",

        "monthly_orders",

        "monthly_revenue",

        "predicted_revenue",

        "forecast_error",

        "absolute_error",

        "error_percent"

    ]

]

----------------------

prediction_df

-------------------

prediction_spark = spark.createDataFrame(

    prediction_df

)

------------------

(

    prediction_spark

    .write

    .mode("overwrite")

    .saveAsTable(

        PREDICTION_TABLE

    )

)

--------------------

display(

    spark.table(

        PREDICTION_TABLE

    )

)

--------------------

print()

print("=" * 70)

print("Sales Forecast Batch Inference Completed")

print()

print(f"Prediction Table : {PREDICTION_TABLE}")

print(f"Rows Predicted : {prediction_df.shape[0]}")

print()

print("=" * 70)

-----------------



