Cell 15
Decision Tree Regressor
dt = DecisionTreeRegressor(

    featuresCol="features",

    labelCol="label",

    predictionCol="prediction",

    seed=42

)
Cell 16

Train Decision Tree

with mlflow.start_run(run_name="Decision Tree"):

    dt_model = dt.fit(train_df)

    dt_predictions = dt_model.transform(test_df)
Cell 17

Evaluate Decision Tree

    dt_rmse = rmse_evaluator.evaluate(dt_predictions)

    dt_mae = mae_evaluator.evaluate(dt_predictions)

    dt_r2 = r2_evaluator.evaluate(dt_predictions)
Cell 18

Log Decision Tree

    mlflow.log_param(
        "Model",
        "Decision Tree"
    )

    mlflow.log_metric(
        "RMSE",
        dt_rmse
    )

    mlflow.log_metric(
        "MAE",
        dt_mae
    )

    mlflow.log_metric(
        "R2",
        dt_r2
    )

    mlflow.spark.log_model(

        spark_model=dt_model,

        artifact_path="model",

        dfs_tmpdir=MLFLOW_TMP_DIR

    )
Cell 19

Store Decision Tree Result

    model_results.append(

        (

            "Decision Tree",

            dt_rmse,

            dt_mae,

            dt_r2

        )

    )
Cell 20

Preview Decision Tree Prediction

display(

    dt_predictions.select(

        "customer_id",

        "label",

        "prediction"

    )

)
Cell 21
Random Forest Regressor
rf = RandomForestRegressor(

    featuresCol="features",

    labelCol="label",

    predictionCol="prediction",

    numTrees=100,

    seed=42

)
Cell 22

Train Random Forest

with mlflow.start_run(run_name="Random Forest"):

    rf_model = rf.fit(train_df)

    rf_predictions = rf_model.transform(test_df)
Cell 23

Evaluate Random Forest

    rf_rmse = rmse_evaluator.evaluate(rf_predictions)

    rf_mae = mae_evaluator.evaluate(rf_predictions)

    rf_r2 = r2_evaluator.evaluate(rf_predictions)
Cell 24

Log Random Forest

    mlflow.log_param(
        "Model",
        "Random Forest"
    )

    mlflow.log_param(
        "Num Trees",
        100
    )

    mlflow.log_metric(
        "RMSE",
        rf_rmse
    )

    mlflow.log_metric(
        "MAE",
        rf_mae
    )

    mlflow.log_metric(
        "R2",
        rf_r2
    )

    mlflow.spark.log_model(

        spark_model=rf_model,

        artifact_path="model",

        dfs_tmpdir=MLFLOW_TMP_DIR

    )
Cell 25

Store Random Forest Result

    model_results.append(

        (

            "Random Forest",

            rf_rmse,

            rf_mae,

            rf_r2

        )

    )
Cell 26

Preview Random Forest Prediction

display(

    rf_predictions.select(

        "customer_id",

        "label",

        "prediction"

    )

)
Cell 27
Gradient Boosted Tree Regressor
gbt = GBTRegressor(

    featuresCol="features",

    labelCol="label",

    predictionCol="prediction",

    maxIter=100,

    seed=42

)
Cell 28

Train GBT

with mlflow.start_run(run_name="Gradient Boosted Trees"):

    gbt_model = gbt.fit(train_df)

    gbt_predictions = gbt_model.transform(test_df)
Cell 29

Evaluate GBT

    gbt_rmse = rmse_evaluator.evaluate(gbt_predictions)

    gbt_mae = mae_evaluator.evaluate(gbt_predictions)

    gbt_r2 = r2_evaluator.evaluate(gbt_predictions)
Cell 30

Log GBT

    mlflow.log_param(
        "Model",
        "Gradient Boosted Trees"
    )

    mlflow.log_param(
        "Max Iterations",
        100
    )

    mlflow.log_metric(
        "RMSE",
        gbt_rmse
    )

    mlflow.log_metric(
        "MAE",
        gbt_mae
    )

    mlflow.log_metric(
        "R2",
        gbt_r2
    )

    mlflow.spark.log_model(

        spark_model=gbt_model,

        artifact_path="model",

        dfs_tmpdir=MLFLOW_TMP_DIR

    )
Cell 31

Store GBT Result

    model_results.append(

        (

            "Gradient Boosted Trees",

            gbt_rmse,

            gbt_mae,

            gbt_r2

        )

    )
Cell 32

Preview GBT Predictions

display(

    gbt_predictions.select(

        "customer_id",

        "label",

        "prediction"

    )

)