import pandas as pd

from pyspark.sql import functions as F
from pyspark.sql.window import Window

--------------

SOURCE_TABLE = "retailmart.ml_db.future_sales_forecast"

OUTPUT_TABLE = "retailmart.ml_db.sales_forecast_dashboard"

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

