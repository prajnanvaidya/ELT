comparison_df.write \

    .format("delta") \

    .mode("overwrite") \

    .saveAsTable(COMPARISON_TABLE)

print("Model comparison table saved.")

--------

best_model = rf_model

best_model.write() \

    .overwrite() \

    .save(MODEL_TABLE)

print("Inventory Risk model saved.")

-----------------------

loaded_model = RandomForestClassificationModel.load(

    MODEL_TABLE

)

print("Saved model loaded successfully.")

--------------------

predictions = loaded_model.transform(model_df)

display(

    predictions.select(

        "inventory_id",

        "product_id",

        "store_id",

        "inventory_risk",

        "prediction",

        "probability"

    )

)

---------------------------

predictions.select(

    "inventory_id",

    "product_id",

    "store_id",

    "inventory_risk",

    "prediction",

    "probability"

).write \

.format("delta") \

.mode("overwrite") \

.saveAsTable(f"{CATALOG}.{SCHEMA}.inventory_risk_predictions")

print("Prediction table saved.")

-------------------------

print("Model Table")

spark.table(MODEL_TABLE).show(5)

print("")

print("Comparison Table")

display(

    spark.table(COMPARISON_TABLE)

)

print("")

print("Prediction Table")

display(

    spark.table(

        f"{CATALOG}.{SCHEMA}.inventory_risk_predictions"

    )

)

-----------------------------