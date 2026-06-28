cv_model = cross_validator.fit(train_df)

------------

best_rf_model = cv_model.bestModel

------------

tuned_predictions = best_rf_model.transform(test_df)

------------

display(
    tuned_predictions
)

------------

tuned_rmse = rmse_evaluator.evaluate(
    tuned_predictions
)

tuned_mae = mae_evaluator.evaluate(
    tuned_predictions
)

tuned_r2 = r2_evaluator.evaluate(
    tuned_predictions
)

------------

print("=" * 60)

print("Tuned Random Forest Performance")

print("=" * 60)

print(f"RMSE : {tuned_rmse:.4f}")

print(f"MAE  : {tuned_mae:.4f}")

print(f"R²   : {tuned_r2:.4f}")

print("=" * 60)

------------

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

------------


if mlflow.active_run():
    mlflow.end_run()

------------

with mlflow.start_run(
    run_name="Random Forest Hyperparameter Tuning"
)

------------

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
    best_rf_model.getMaxDepth()
)

mlflow.log_param(
    "maxBins",
    best_rf_model.getMaxBins()
)

mlflow.log_param(
    "numFolds",
    3
)

mlflow.log_param(
    "Grid Size",
    len(param_grid)
)

mlflow.log_param(
    "Training Rows",
    train_df.count()
)

mlflow.log_param(
    "Testing Rows",
    test_df.count()
)

------------

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

------------

mlflow.spark.log_model(

    spark_model=best_rf_model,

    artifact_path="model",

    dfs_tmpdir=MLFLOW_TMP_DIR

)


