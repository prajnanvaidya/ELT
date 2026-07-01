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

history = sales_pd.copy()

future_predictions = []

last_row = history.iloc[-1].copy()

last_date = pd.to_datetime(last_row["forecast_date"])

-------------------

for i in range(FORECAST_MONTHS):

    next_date = last_date + pd.DateOffset(months=1)

    year = next_date.year

    month = next_date.month

    quarter = ((month - 1) // 3) + 1

    monthly_orders = last_row["monthly_orders"]

    lag_1_revenue = last_row["monthly_revenue"]

    lag_2_revenue = last_row["lag_1_revenue"]

    lag_3_revenue = last_row["lag_2_revenue"]

    lag_1_orders = last_row["monthly_orders"]

    rolling_avg_revenue = (

        lag_1_revenue +

        lag_2_revenue +

        lag_3_revenue

    ) / 3

    revenue_growth_pct = (

        (lag_1_revenue - lag_2_revenue)

        /

        lag_2_revenue

    ) * 100

    feature_df = pd.DataFrame({

        "year":[year],

        "month":[month],

        "monthly_orders":[monthly_orders],

        "lag_1_revenue":[lag_1_revenue],

        "lag_2_revenue":[lag_2_revenue],

        "lag_3_revenue":[lag_3_revenue],

        "lag_1_orders":[lag_1_orders],

        "rolling_avg_revenue":[rolling_avg_revenue],

        "revenue_growth_pct":[revenue_growth_pct],

        "quarter":[quarter]

    })

    predicted_revenue = model.predict(feature_df)[0]

    new_row = {

        "forecast_date":next_date,

        "year":year,

        "month":month,

        "monthly_revenue":predicted_revenue,

        "monthly_orders":monthly_orders,

        "lag_1_revenue":lag_1_revenue,

        "lag_2_revenue":lag_2_revenue,

        "lag_3_revenue":lag_3_revenue,

        "lag_1_orders":lag_1_orders,

        "rolling_avg_revenue":rolling_avg_revenue,

        "revenue_growth_pct":revenue_growth_pct,

        "quarter":quarter

    }

    future_predictions.append(new_row)

    history = pd.concat(

        [

            history,

            pd.DataFrame([new_row])

        ],

        ignore_index=True

    )

    last_row = history.iloc[-1]

    last_date = next_date

------------------

forecast_pd = pd.DataFrame(future_predictions)

forecast_pd

-------------

print()

print("="*70)

print("Future Forecast Generated")

print()

print("Forecast Months :", len(forecast_pd))

print()

print(forecast_pd[

    [

        "forecast_date",

        "monthly_revenue"

    ]

])

print()

print("="*70)

-----------------------