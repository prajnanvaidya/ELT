print("Dashboard Detail Rows")

print(

    spark.table(DETAIL_TABLE).count()

)

print("")

print("Dashboard Summary Rows")

print(

    spark.table(SUMMARY_TABLE).count()

)

print("")

print("Category Summary Rows")

print(

    spark.table(CATEGORY_TABLE).count()

)

print("")

print("Store Summary Rows")

print(

    spark.table(STORE_TABLE).count()

)

print("")

print("Risk Distribution Rows")

print(

    spark.table(RISK_TABLE).count()

)

----------------------------------

dashboard_df.select(

    [

        count(

            when(

                col(c).isNull(),

                c

            )

        ).alias(c)

        for c in dashboard_df.columns

    ]

).display()

--------------------------

dashboard_df.groupBy(

    "predicted_risk"

).count().display()

-------------------------

dashboard_df.groupBy(

    "actual_risk"

).count().display()

--------------------

dashboard_df.groupBy(

    "actual_risk",

    "predicted_risk"

).count().display()

-------------------

print("=" * 60)

print("Inventory Dashboard Tables Created Successfully")

print("=" * 60)

print("Detail Table")

print(DETAIL_TABLE)

print("")

print("Summary Table")

print(SUMMARY_TABLE)

print("")

print("Category Summary")

print(CATEGORY_TABLE)

print("")

print("Store Summary")

print(STORE_TABLE)

print("")

print("Risk Distribution")

print(RISK_TABLE)

print("=" * 60)

-----------------------------------

