18.------------

cv_model = cross_validator.fit(train_df)

19.------------

best_rf_model = cv_model.bestModel

20.------------

tuned_predictions = best_rf_model.transform(test_df)

21.------------

display(
    tuned_predictions
)

22.------------

tuned_rmse = rmse_evaluator.evaluate(
    tuned_predictions
)

tuned_mae = mae_evaluator.evaluate(
    tuned_predictions
)

tuned_r2 = r2_evaluator.evaluate(
    tuned_predictions
)

23.------------

print("=" * 60)

print("Tuned Random Forest Performance")

print("=" * 60)

print(f"RMSE : {tuned_rmse:.4f}")

print(f"MAE  : {tuned_mae:.4f}")

print(f"R²   : {tuned_r2:.4f}")

print("=" * 60)

24.------------

print("=" * 60)

print("Best Hyperparameters")

print("=" * 60)

print(
    "Number of Trees :",
    best_rf_model.getNumTrees
)

print(
    "Maximum Depth :",
    best_rf_model.getMaxDepth()
)

print(
    "Maximum Bins :",
    best_rf_model.getMaxBins()
)

print("=" * 60)

25.------------


if mlflow.active_run():
    mlflow.end_run()

26.------------

with mlflow.start_run(run_name="Random Forest Hyperparameter Tuning"):
    mlflow.set_tag(
        "model_type",
        "tuned_random_forest"
    )

    mlflow.set_tag(
        "project",
        "RetailMart"
    )

    mlflow.set_tag(
        "stage",
        "production_candidate"
    )

    mlflow.log_param(
        "Model",
        "Random Forest"
    )

    mlflow.log_param(
        "numTrees",
        best_rf_model.getNumTrees()
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

    sample_input = train_df.limit(10).toPandas()

    sample_prediction = (
        best_rf_model
        .transform(train_df.limit(10))
        .select("prediction")
        .toPandas()
    )

    signature = infer_signature(
        sample_input,
        sample_prediction
    )

    mlflow.spark.log_model(

        spark_model=best_rf_model,

        artifact_path="model",

        signature=signature,

        dfs_tmpdir=MLFLOW_TMP_DIR

    )







