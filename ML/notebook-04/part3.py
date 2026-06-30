27.-----------------------------

tuned_model_df = spark.createDataFrame(

    [

        (

            "Random Forest (Tuned)",

            float(tuned_rmse),

            float(tuned_mae),

            float(tuned_r2),

            len(best_rf_model.trees),

            best_rf_model.getOrDefault(best_rf_model.maxDepth),

            best_rf_model.getOrDefault(best_rf_model.maxBins)

        )

    ],

    [

        "Model",

        "RMSE",

        "MAE",

        "R2",

        "Num_Trees",

        "Max_Depth",

        "Max_Bins"

    ]

)

28.-------------

display(
    tuned_model_df
)

29.------------

tuned_model_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(

        f"{CATALOG}.{ML_SCHEMA}.tuned_customer_spend_model"

    )

30.--------

display(

    spark.table(

        f"{CATALOG}.{ML_SCHEMA}.tuned_customer_spend_model"

    )

)

31.-------

base_rmse = spark.table(

    f"{CATALOG}.{ML_SCHEMA}.best_customer_spend_model"

).first()["RMSE"]

improvement = base_rmse - tuned_rmse

improvement_percent = (improvement / base_rmse) * 100

32.---------

print("=" * 60)

print("Model Comparison")

print("=" * 60)

print(f"Base Model RMSE      : {base_rmse:.4f}")

print(f"Tuned Model RMSE     : {tuned_rmse:.4f}")

print(f"Improvement          : {improvement:.4f}")

print(f"Improvement (%)      : {improvement_percent:.2f}%")

print("=" * 60)

33.---------

print("=" * 60)

print("Notebook 04 Completed Successfully")

print("=" * 60)

print("✓ Hyperparameter Tuning Completed")

print("✓ Tuned Random Forest Saved")

print("✓ MLflow Logged")

print("✓ Delta Tables Updated")

print("=" * 60)

end.----------------------