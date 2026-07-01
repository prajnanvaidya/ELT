import pandas as pd
import numpy as np

from sklearn.linear_model import LinearRegression

from pyspark.sql import functions as F

-----------------

SOURCE_TABLE = "retailmart.ml_db.sales_forecast_features"

FORECAST_TABLE = "retailmart.ml_db.sales_future_forecast"

FORECAST_MONTHS = 6

-----------------

sales_pd = (

    spark.table(SOURCE_TABLE)

    .orderBy("forecast_date")

    .toPandas()

)

sales_pd.tail()

-----------------

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

----------------

model = LinearRegression()

model.fit(

    sales_pd[feature_columns],

    sales_pd[target_column]

)

print()

print("="*70)

print("Forecast Model Ready")

print()

print("Training Rows :", len(sales_pd))

print()

print("="*70)

------------------