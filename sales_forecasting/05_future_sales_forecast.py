import pandas as pd
import numpy as np

from sklearn.linear_model import LinearRegression

from pyspark.sql import functions as F

-----------------

SOURCE_TABLE = "retailmart.gold.gold_monthly_sales"

FORECAST_TABLE = "retailmart.ml_db.sales_future_forecast"

FORECAST_MONTHS = 6

-------------

sales_pd = (

    spark.table(SOURCE_TABLE)

    .orderBy("year", "month")

    .toPandas()

)

--------------

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

-----------------

model = LinearRegression()

model.fit(

    sales_pd[feature_columns],

    sales_pd[target_column]

)

------------------

history = sales_pd.copy()

future_rows = []

last_date = pd.to_datetime(history["forecast_date"].iloc[-1])

------------------

for i in range(FORECAST_MONTHS):

    next_date = last_date + pd.DateOffset(months=1)

    year = next_date.year

    month = next_date.month

    quarter = next_date.quarter

    monthly_orders = history["monthly_orders"].iloc[-1]

    lag1 = history["monthly_revenue"].iloc[-1]

    lag2 = history["monthly_revenue"].iloc[-2]

    lag3 = history["monthly_revenue"].iloc[-3]

    rolling_avg = history["monthly_revenue"].tail(3).mean()

    growth = (

        (lag1 - lag2)

        /

        lag2

    ) * 100

    features = pd.DataFrame({

        "year":[year],

        "month":[month],

        "monthly_orders":[monthly_orders],

        "lag_1_revenue":[lag1],

        "lag_2_revenue":[lag2],

        "lag_3_revenue":[lag3],

        "lag_1_orders":[monthly_orders],

        "rolling_avg_revenue":[rolling_avg],

        "revenue_growth_pct":[growth],

        "quarter":[quarter]

    })

    prediction = model.predict(features)[0]

    future_rows.append({

        "forecast_date":next_date,

        "year":year,

        "month":month,

        "quarter":quarter,

        "forecast_orders":monthly_orders,

        "predicted_revenue":prediction

    })

    history.loc[len(history)] = {

        **history.iloc[-1].to_dict(),

        "forecast_date":next_date,

        "year":year,

        "month":month,

        "quarter":quarter,

        "monthly_orders":monthly_orders,

        "monthly_revenue":prediction

    }

    last_date = next_date

----------------

forecast_df = pd.DataFrame(future_rows)

forecast_df

---------------

forecast_spark = spark.createDataFrame(forecast_df)

---------------

(

forecast_spark.write

.mode("overwrite")

.format("delta")

.saveAsTable(FORECAST_TABLE)

)

---------------

display(

spark.table(

FORECAST_TABLE

)

)

----------------

print()

print("="*70)

print("Future Sales Forecast Completed")

print()

print(f"Forecast Months : {FORECAST_MONTHS}")

print(f"Forecast Table : {FORECAST_TABLE}")

print()

print("="*70)

--------------------

