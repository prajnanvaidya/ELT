from pyspark.sql import functions as F

import pandas as pd

import mlflow
import mlflow.sklearn

from sklearn.linear_model import LinearRegression

----------------------

CATALOG = "retailmart"

ML_SCHEMA = "ml_db"

FEATURE_TABLE = f"{CATALOG}.{ML_SCHEMA}.sales_forecast_features"

MODEL_TABLE = f"{CATALOG}.{ML_SCHEMA}.best_sales_forecast_model"

REGISTERED_MODEL = "sales_forecast_model"

--------------

sales_df = spark.table(FEATURE_TABLE)

display(sales_df)

----------------

sales_pd = (

    sales_df

    .orderBy("forecast_date")

    .toPandas()

)

-------------------

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

-------------------------

X = sales_pd[feature_columns]

y = sales_pd[target_column]

----------------

model = LinearRegression()

model.fit(

    X,

    y

)

------------------

mlflow.set_experiment(

    "/Users/prajnan.vaidya@fractal.ai/sales_forecast"

)

--------------

with mlflow.start_run(run_name="Linear Regression"):

    mlflow.log_param(

        "model",

        "Linear Regression"

    )

    mlflow.log_param(

        "features",

        len(feature_columns)

    )

    mlflow.sklearn.log_model(

        sk_model=model,

        artifact_path="sales_forecast_model"

    )

    run_id = mlflow.active_run().info.run_id

-----------------

model_df = spark.createDataFrame(

    [

        (

            "Linear Regression",

            run_id,

            len(feature_columns)

        )

    ],

    [

        "model_name",

        "run_id",

        "feature_count"

    ]

)

-------------------

(

    model_df

    .write

    .mode("overwrite")

    .saveAsTable(

        MODEL_TABLE

    )

)

-------------------

display(

    spark.table(

        MODEL_TABLE

    )

)

----------------

print()

print("="*70)

print("Sales Forecast Model Registered")

print()

print("Model")

print("Linear Regression")

print()

print("Features")

print(len(feature_columns))

print()

print("Run ID")

print(run_id)

print("="*70)

-------------------

