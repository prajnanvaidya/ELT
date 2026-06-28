if mlflow.active_run():
    mlflow.end_run()

with mlflow.start_run(run_name="Tuned Random Forest"):

    mlflow.log_param(
        "Model",
        "Random Forest (Tuned)"
    )

    mlflow.log_param(
        "numTrees",
        len(best_rf_model.trees)
    )

    mlflow.log_param(
        "maxDepth",
        best_rf_model.getOrDefault(best_rf_model.maxDepth)
    )

    mlflow.log_param(
        "maxBins",
        best_rf_model.getOrDefault(best_rf_model.maxBins)
    )

    mlflow.log_param(
        "numFolds",
        3
    )

    mlflow.log_param(
        "Grid Size",
        len(param_grid)
    )

    mlflow.log_metric(
        "RMSE",
        tuned_rmse
    )

    mlflow.log_metric(
        "MAE",
        tuned_mae
    )

    mlflow.log_metric(
        "R2",
        tuned_r2
    )

    mlflow.spark.log_model(
        spark_model=best_rf_model,
        artifact_path="model",
        dfs_tmpdir=MLFLOW_TMP_DIR
    )

-------------

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

-------------

display(
    tuned_model_df
)

------------

tuned_model_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(

        f"{CATALOG}.{ML_SCHEMA}.tuned_customer_spend_model"

    )

--------

display(

    spark.table(

        f"{CATALOG}.{ML_SCHEMA}.tuned_customer_spend_model"

    )

)

-------

base_rmse = spark.table(

    f"{CATALOG}.{ML_SCHEMA}.best_customer_spend_model"

).first()["RMSE"]

improvement = base_rmse - tuned_rmse

improvement_percent = (improvement / base_rmse) * 100

---------

print("=" * 60)

print("Model Comparison")

print("=" * 60)

print(f"Base Model RMSE      : {base_rmse:.4f}")

print(f"Tuned Model RMSE     : {tuned_rmse:.4f}")

print(f"Improvement          : {improvement:.4f}")

print(f"Improvement (%)      : {improvement_percent:.2f}%")

print("=" * 60)

---------

print("=" * 60)

print("Notebook 04 Completed Successfully")

print("=" * 60)

print("✓ Hyperparameter Tuning Completed")

print("✓ Tuned Random Forest Saved")

print("✓ MLflow Logged")

print("✓ Delta Tables Updated")

print("=" * 60)