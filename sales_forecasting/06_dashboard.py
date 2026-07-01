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

