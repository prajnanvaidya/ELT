# Sales Forecasting - Feature Engineering

## Objective

This notebook prepares machine learning features for monthly sales forecasting.

Features created:
- Forecast Date
- Year
- Month
- Quarter
- Lag Features
- Rolling Average Features
- Revenue Growth
- Previous Month Orders

Output Table:
retailmart.ml_db.sales_forecast_features

--------------------------------------------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

----------------------------------

CATALOG = "retailmart"

ML_SCHEMA = "ml_db"

SOURCE_TABLE = f"{CATALOG}.{ML_SCHEMA}.gold_monthly_sales"

FEATURE_TABLE = f"{CATALOG}.{ML_SCHEMA}.sales_forecast_features"

-------------------------------------

sales_df = spark.table(SOURCE_TABLE)

display(sales_df)

-----------------

sales_df.printSchema()

------------------

sales_df = (

    sales_df

    .withColumn(

        "forecast_date",

        F.to_date(

            F.concat_ws(

                "-",

                F.col("year"),

                F.lpad(F.col("month"),2,"0"),

                F.lit("01")

            )

        )

    )

)

----------------------

sales_df = sales_df.orderBy("forecast_date")

-----------------

window_spec = Window.orderBy("forecast_date")

---------------

sales_df = (

    sales_df

    .withColumn(

        "lag_1_revenue",

        F.lag("monthly_revenue",1).over(window_spec)

    )

    .withColumn(

        "lag_2_revenue",

        F.lag("monthly_revenue",2).over(window_spec)

    )

    .withColumn(

        "lag_3_revenue",

        F.lag("monthly_revenue",3).over(window_spec)

    )

)

-------------------

sales_df = (

    sales_df

    .withColumn(

        "lag_1_orders",

        F.lag("monthly_orders",1).over(window_spec)

    )

)

-----------------

rolling_window = window_spec.rowsBetween(-3,-1)

sales_df = (

    sales_df

    .withColumn(

        "rolling_avg_revenue",

        F.avg(

            "monthly_revenue"

        ).over(

            rolling_window

        )

    )

)

----------------

sales_df = (

    sales_df

    .withColumn(

        "revenue_growth_pct",

        (

            (

                F.col("monthly_revenue")

                -

                F.col("lag_1_revenue")

            )

            /

            F.col("lag_1_revenue")

        )*100

    )

)

-----------------

sales_df = (

    sales_df

    .withColumn(

        "quarter",

        F.quarter("forecast_date")

    )

)

---------------

sales_df = sales_df.dropna()

------------

display(sales_df)

-------------

(

    sales_df

    .write

    .format("delta")

    .mode("overwrite")

    .saveAsTable(

        FEATURE_TABLE

    )

)

----------------

display(

    spark.table(

        FEATURE_TABLE

    )

)

----------------

print("="*70)

print("Sales Forecast Feature Engineering Completed")

print("="*70)

print()

print(f"Source Table  : {SOURCE_TABLE}")

print(f"Feature Table : {FEATURE_TABLE}")

print(f"Rows          : {sales_df.count()}")

print(f"Columns       : {len(sales_df.columns)}")

print("="*70)

------------------

