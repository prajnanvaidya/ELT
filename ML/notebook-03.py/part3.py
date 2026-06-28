cell 33

comparison_df = spark.createDataFrame(

    model_results,

    [

        "Model",

        "RMSE",

        "MAE",

        "R2"

    ]

)

cell-34

display(

    comparison_df

)

cell-35

(
    comparison_df

    .write

    .format("delta")

    .mode("overwrite")

    .saveAsTable(

        f"{CATALOG}.{ML_SCHEMA}.model_comparison_results"

    )

)

cell-36

display(

    spark.table(

        f"{CATALOG}.{ML_SCHEMA}.model_comparison_results"

    )

)


cell-37

best_model = (

    comparison_df

    .orderBy("RMSE")

    .first()

)


cell-38

print("=" * 60)

print("Best Performing Model")

print("=" * 60)

print(f"Model : {best_model['Model']}")

print(f"RMSE  : {best_model['RMSE']:.4f}")

print(f"MAE   : {best_model['MAE']:.4f}")

print(f"R²    : {best_model['R2']:.4f}")

print("=" * 60)


cell-39

best_model_df = spark.createDataFrame(

    [

        (

            best_model["Model"],

            float(best_model["RMSE"]),

            float(best_model["MAE"]),

            float(best_model["R2"])

        )

    ],

    [

        "Model",

        "RMSE",

        "MAE",

        "R2"

    ]

)


cell-40

(
    best_model_df

    .write

    .format("delta")

    .mode("overwrite")

    .saveAsTable(

        f"{CATALOG}.{ML_SCHEMA}.best_customer_spend_model"

    )

)


cell-41

display(

    spark.table(

        f"{CATALOG}.{ML_SCHEMA}.best_customer_spend_model"

    )

)




cell-12

mlflow.log_param(
    "Model",
    "Linear Regression"
)

mlflow.log_param(
    "maxIter",
    lr.getMaxIter()
)

mlflow.log_param(
    "regParam",
    lr.getRegParam()
)

mlflow.log_param(
    "elasticNetParam",
    lr.getElasticNetParam()
)

mlflow.log_param(
    "Training Rows",
    train_df.count()
)

mlflow.log_param(
    "Testing Rows",
    test_df.count()
)



cell-18

mlflow.log_param(
    "Model",
    "Decision Tree"
)

mlflow.log_param(
    "maxDepth",
    dt.getMaxDepth()
)

mlflow.log_param(
    "maxBins",
    dt.getMaxBins()
)

mlflow.log_param(
    "Training Rows",
    train_df.count()
)

mlflow.log_param(
    "Testing Rows",
    test_df.count()
)


cell-24

mlflow.log_param(
    "Model",
    "Random Forest"
)

mlflow.log_param(
    "numTrees",
    rf.getNumTrees()
)

mlflow.log_param(
    "maxDepth",
    rf.getMaxDepth()
)

mlflow.log_param(
    "maxBins",
    rf.getMaxBins()
)

mlflow.log_param(
    "Training Rows",
    train_df.count()
)

mlflow.log_param(
    "Testing Rows",
    test_df.count()
)


cell-30

mlflow.log_param(
    "Model",
    "Gradient Boosted Trees"
)

mlflow.log_param(
    "maxIter",
    gbt.getMaxIter()
)

mlflow.log_param(
    "maxDepth",
    gbt.getMaxDepth()
)

mlflow.log_param(
    "maxBins",
    gbt.getMaxBins()
)

mlflow.log_param(
    "Training Rows",
    train_df.count()
)

mlflow.log_param(
    "Testing Rows",
    test_df.count()
)



