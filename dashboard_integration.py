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

loyalty_df = (

    spark.table(
        f"{CATALOG}.{ML_SCHEMA}.customer_features_ml"
    )

    .select(
        "customer_id",
        "loyalty_level"
    )

)

--------------------------

loyalty_summary = (

    prediction_df

    .join(
        loyalty_df,
        "customer_id"
    )

    .groupBy(
        "loyalty_level"
    )

    .agg(

        F.count("*").alias("customers"),

        F.avg("prediction").alias("avg_predicted_spend"),

        F.avg("label").alias("avg_actual_spend")

    )

    .orderBy("loyalty_level")

)

---------------------------

(
    loyalty_summary

    .write

    .format("delta")

    .mode("overwrite")

    .saveAsTable(

        f"{CATALOG}.{ML_SCHEMA}.customer_spend_loyalty_summary"

    )

)

--------------------------

display(

    spark.table(

        f"{CATALOG}.{ML_SCHEMA}.customer_spend_loyalty_summary"

    )

)

------------------------------

top_customers = (

    prediction_df

    .select(

        "customer_id",

        "prediction",

        "label"

    )

    .orderBy(

        F.desc("prediction")

    )

    .limit(10)

)

------------------------------

(
    top_customers

    .write

    .format("delta")

    .mode("overwrite")

    .saveAsTable(

        f"{CATALOG}.{ML_SCHEMA}.customer_spend_top_customers"

    )

)

---------------------------------

display(

    spark.table(

        f"{CATALOG}.{ML_SCHEMA}.customer_spend_top_customers"

    )

)

----------------------------

summary_df = prediction_df.agg(

    F.count("*").alias("total_customers"),

    F.avg("prediction").alias("average_prediction"),

    F.max("prediction").alias("highest_prediction"),

    F.min("prediction").alias("lowest_prediction")

)

------------------------------

summary_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        f"{CATALOG}.{ML_SCHEMA}.customer_prediction_summary"
    )

------------------------------

display(

    spark.table(

        f"{CATALOG}.{ML_SCHEMA}.customer_prediction_summary"

    )

)

-----------------------------

dashboard_tables = [

    "customer_spend_predictions",

    "customer_prediction_summary",

    "customer_spend_prediction_distribution",

    "customer_spend_loyalty_summary",

    "customer_spend_top_customers"

]

dashboard_df = spark.createDataFrame(

    [(table,) for table in dashboard_tables],

    ["Dashboard Table"]

)

----------------------------------

display(dashboard_df)

-------------------------

print("=" * 60)

print("Notebook 09 Completed Successfully")

print("=" * 60)

print("✓ Dashboard Tables Created")

print("✓ Prediction Summary Created")

print("✓ Distribution Table Created")

print("✓ Loyalty Summary Created")

print("✓ Top Customers Table Created")

print("✓ Ready for Databricks SQL Dashboard")

print("=" * 60)