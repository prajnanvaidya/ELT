import pandas as pd

from pyspark.sql import functions as F
from pyspark.sql.window import Window

--------------

SOURCE_TABLE = "retailmart.ml_db.future_sales_forecast"

OUTPUT_TABLE = "retailmart.ml_db.sales_forecast_dashboard"

--------------

forecast_df = spark.table(

    SOURCE_TABLE

)

display(forecast_df)

----------------

dashboard_df = (

    forecast_df

    .withColumn(

        "month_name",

        F.date_format(

            "forecast_date",

            "MMMM"

        )

    )

    .withColumn(

        "forecast_year",

        F.year(

            "forecast_date"

        )

    )

    .withColumn(

        "forecast_quarter",

        F.concat(

            F.lit("Q"),

            F.quarter(

                "forecast_date"

            )

        )

    )

    .withColumn(

        "forecast_label",

        F.concat_ws(

            " ",

            F.date_format(

                "forecast_date",

                "MMM"

            ),

            F.year(

                "forecast_date"

            )

        )

    )

)

-----------------

window_spec = Window.orderBy(

    "forecast_date"

)

dashboard_df = (

    dashboard_df

    .withColumn(

        "previous_forecast",

        F.lag(

            "forecast_revenue"

        ).over(

            window_spec

        )

    )

    .withColumn(

        "forecast_growth_percent",

        F.round(

            (

                (

                    F.col(

                        "forecast_revenue"

                    )

                    -

                    F.col(

                        "previous_forecast"

                    )

                )

                /

                F.col(

                    "previous_forecast"

                )

            ) * 100,

            2

        )

    )

)

-----------------

dashboard_df = (

    dashboard_df

    .withColumn(

        "dashboard_refresh_time",

        F.current_timestamp()

    )

)

--------------

display(

    dashboard_df

)

-----------------

(

    dashboard_df

    .write

    .format("delta")

    .mode("overwrite")

    .saveAsTable(

        OUTPUT_TABLE

    )

)

--------------

display(

    spark.table(

        OUTPUT_TABLE

    )

)

----------

summary_df = (

    dashboard_df

    .agg(

        F.sum(

            "forecast_revenue"

        ).alias(

            "total_forecast_revenue"

        ),

        F.avg(

            "forecast_revenue"

        ).alias(

            "average_forecast_revenue"

        ),

        F.max(

            "forecast_revenue"

        ).alias(

            "highest_forecast_revenue"

        ),

        F.min(

            "forecast_revenue"

        ).alias(

            "lowest_forecast_revenue"

        ),

        F.avg(

            "forecast_growth_percent"

        ).alias(

            "average_growth_percent"

        )

    )

    .withColumn(

        "dashboard_refresh_time",

        F.current_timestamp()

    )

)

---------------

summary_df.write \

    .format("delta") \

    .mode("overwrite") \

    .saveAsTable(

        "retailmart.ml_db.sales_forecast_dashboard_summary"

    )

----------------

display(

    spark.table(

        "retailmart.ml_db.sales_forecast_dashboard_summary"

    )

)

------------------

print("=" * 70)

print("Sales Forecast Dashboard Table Created Successfully")

print("=" * 70)

print()

print(f"Forecast Table  : {OUTPUT_TABLE}")

print("Summary Table   : retailmart.ml_db.sales_forecast_dashboard_summary")

print()

print("Ready for Dashboard")

print("=" * 70)

------------------------


