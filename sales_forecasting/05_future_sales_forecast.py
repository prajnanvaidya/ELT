import pandas as pd
import numpy as np

from sklearn.linear_model import LinearRegression

---------------

SOURCE_TABLE = "retailmart.ml_db.sales_forecast_features"

OUTPUT_TABLE = "retailmart.ml_db.future_sales_forecast"

FORECAST_MONTHS = 6

----------------

sales_pd = (
    spark.table(SOURCE_TABLE)
    .orderBy("forecast_date")
    .toPandas()
)

sales_pd.tail()

-------------------

feature_columns = [

    "year",

    "month",

    "monthly_orders"

]

target_column = "monthly_revenue"

----------------

model = LinearRegression()

model.fit(

    sales_pd[feature_columns],

    sales_pd[target_column]

)

print()

print("=" * 70)

print("Forecast Model Ready")

print()

print("Training Rows :", len(sales_pd))

print()

print("=" * 70)

---------------

history = sales_pd.copy()

last_date = pd.to_datetime(history["forecast_date"].iloc[-1])

last_orders = history["monthly_orders"].iloc[-1]

future_predictions = []

------------------

for i in range(FORECAST_MONTHS):

    future_date = last_date + pd.DateOffset(months=i + 1)

    year = future_date.year

    month = future_date.month

    # Smooth order growth
    growth = 1 + np.random.uniform(-0.02, 0.03)

    last_orders = int(last_orders * growth)

    X = pd.DataFrame({

        "year": [year],

        "month": [month],

        "monthly_orders": [last_orders]

    })

    predicted = model.predict(X)[0]

    # Safety checks

    predicted = max(predicted, 0)

    previous = history["monthly_revenue"].iloc[-1]

    lower = previous * 0.95

    upper = previous * 1.08

    predicted = np.clip(predicted, lower, upper)

    future_predictions.append({

        "forecast_date": future_date,

        "year": year,

        "month": month,

        "monthly_orders": last_orders,

        "forecast_revenue": round(predicted, 2)

    })

    history.loc[len(history)] = [

        year,

        month,

        predicted,

        last_orders,

        future_date,

        np.nan,

        np.nan,

        np.nan,

        np.nan,

        np.nan,

        np.nan,

        np.nan

    ]

---------------------

forecast_pd = pd.DataFrame(future_predictions)

forecast_pd

----------------------

forecast_spark = spark.createDataFrame(forecast_pd)

forecast_spark.write \

    .mode("overwrite") \

    .format("delta") \

    .saveAsTable(OUTPUT_TABLE)

-----------------------

display(

    spark.table(

        OUTPUT_TABLE

    )

)

----------------------

print()

print("=" * 70)

print("Future Sales Forecast Completed")

print()

print("Forecast Months :", FORECAST_MONTHS)

print()

print("Output Table :", OUTPUT_TABLE)

print()

print("=" * 70)

---------------------

