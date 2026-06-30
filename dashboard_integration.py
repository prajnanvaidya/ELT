# RetailMart ML

## Notebook 09 - Dashboard Integration

### Objective

Prepare dashboard-ready Delta tables
for Customer Spend Prediction.

This notebook creates:

• KPI Table

• Prediction Distribution

• Loyalty Level Summary

• Top Customers

• Model Performance Summary

---------------------------

from pyspark.sql import functions as F

-----------------------------

CATALOG = "retailmart"

ML_SCHEMA = "ml_db"

----------------------------

prediction_df = spark.table(

    f"{CATALOG}.{ML_SCHEMA}.customer_spend_predictions"

)

---------------------------

monitoring_df = spark.table(

    f"{CATALOG}.{ML_SCHEMA}.model_monitoring_results"

)

------------------

kpi_df = prediction_df.agg(

    F.count("*").alias("total_predictions"),

    F.avg("prediction").alias("average_predicted_spend"),

    F.avg("label").alias("average_actual_spend"),

    F.max("prediction").alias("maximum_predicted_spend"),

    F.min("prediction").alias("minimum_predicted_spend")

).withColumn(

    "dashboard_refresh_time",

    F.current_timestamp()

)

-----------------------------

(
    kpi_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        f"{CATALOG}.{ML_SCHEMA}.customer_spend_dashboard_kpi"
    )
)

------------------------

display(

    spark.table(

        f"{CATALOG}.{ML_SCHEMA}.customer_spend_dashboard_kpi"

    )

)

-----------------------

distribution_df = (

    prediction_df

    .withColumn(

        "prediction_band",

        F.when(

            F.col("prediction") < 10000,

            "Below 10K"

        ).when(

            F.col("prediction") < 25000,

            "10K - 25K"

        ).when(

            F.col("prediction") < 50000,

            "25K - 50K"

        ).when(

            F.col("prediction") < 100000,

            "50K - 100K"

        ).otherwise(

            "Above 100K"

        )

    )

    .groupBy("prediction_band")

    .count()

    .orderBy("prediction_band")

)

------------------------------

(
    distribution_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(

        f"{CATALOG}.{ML_SCHEMA}.customer_spend_prediction_distribution"

    )
)

-------------------------------

display(

    spark.table(

        f"{CATALOG}.{ML_SCHEMA}.customer_spend_prediction_distribution"

    )

)

------------------------------------