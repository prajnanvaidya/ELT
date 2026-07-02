summary_df = (

    dashboard_df

    .agg(

        count("*").alias("total_inventory"),

        sum(

            when(col("actual_risk") == "High Risk", 1)

            .otherwise(0)

        ).alias("high_risk_inventory"),

        sum(

            when(col("actual_risk") == "Medium Risk", 1)

            .otherwise(0)

        ).alias("medium_risk_inventory"),

        sum(

            when(col("actual_risk") == "Low Risk", 1)

            .otherwise(0)

        ).alias("low_risk_inventory"),

        round(

            avg("stock_quantity"),

            2

        ).alias("average_stock"),

        round(

            avg("inventory_buffer"),

            2

        ).alias("average_inventory_buffer"),

        round(

            avg("stock_to_reorder_ratio"),

            2

        ).alias("average_stock_to_reorder_ratio")

    )

)

----------------------------------

summary_df.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable(SUMMARY_TABLE)

print("Inventory Dashboard Summary Saved.")

-------------------------------

category_summary = (

    dashboard_df

    .groupBy(

        "category_index"

    )

    .agg(

        count("*").alias("total_products"),

        sum(

            when(col("actual_risk") == "High Risk",1)

            .otherwise(0)

        ).alias("high_risk"),

        sum(

            when(col("actual_risk") == "Medium Risk",1)

            .otherwise(0)

        ).alias("medium_risk"),

        sum(

            when(col("actual_risk") == "Low Risk",1)

            .otherwise(0)

        ).alias("low_risk"),

        round(

            avg("stock_quantity"),

            2

        ).alias("average_stock"),

        round(

            avg("inventory_buffer"),

            2

        ).alias("average_buffer")

    )

)

-------------------------

category_summary.write \

.format("delta") \

.mode("overwrite") \

.saveAsTable(CATEGORY_TABLE)

print("Category Summary Saved.")

------------------------------

store_summary = (

    dashboard_df

    .groupBy(

        "store_id",

        "city_index",

        "state_index"

    )

    .agg(

        count("*").alias("inventory_count"),

        sum(

            when(col("actual_risk")=="High Risk",1)

            .otherwise(0)

        ).alias("high_risk_inventory"),

        sum(

            when(col("actual_risk")=="Medium Risk",1)

            .otherwise(0)

        ).alias("medium_risk_inventory"),

        sum(

            when(col("actual_risk")=="Low Risk",1)

            .otherwise(0)

        ).alias("low_risk_inventory"),

        round(

            avg("stock_quantity"),

            2

        ).alias("average_stock"),

        round(

            avg("inventory_buffer"),

            2

        ).alias("average_buffer")

    )

)

--------------------------------------

store_summary.write \

.format("delta") \

.mode("overwrite") \

.saveAsTable(STORE_TABLE)

print("Store Summary Saved.")

--------------------------------

risk_distribution = (

    dashboard_df

    .groupBy(

        "actual_risk"

    )

    .agg(

        count("*").alias("inventory_count")

    )

)

--------------------------------------

total_inventory = dashboard_df.count()

risk_distribution = (

    risk_distribution

    .withColumn(

        "percentage",

        round(

            col("inventory_count")

            * 100.0

            / total_inventory,

            2

        )

    )

)

----------------------------------------

risk_distribution.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable(RISK_TABLE)

print("Risk Distribution Saved.")

-----------------------------

print("Dashboard Detail")

display(

    spark.table(DETAIL_TABLE)

)

print("")

print("Dashboard Summary")

display(

    spark.table(SUMMARY_TABLE)

)

print("")

print("Category Summary")

display(

    spark.table(CATEGORY_TABLE)

)

print("")

print("Store Summary")

display(

    spark.table(STORE_TABLE)

)

print("")

print("Risk Distribution")

display(

    spark.table(RISK_TABLE)

)

---------------------------------------