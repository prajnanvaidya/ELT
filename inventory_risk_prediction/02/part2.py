lr = LogisticRegression(

    featuresCol="features",

    labelCol="label",

    predictionCol="prediction",

    probabilityCol="probability",

    maxIter=100

)

lr_model = lr.fit(train_df)

lr_predictions = lr_model.transform(test_df)

lr_accuracy = evaluator.evaluate(

    lr_predictions,

    {

        evaluator.metricName: "accuracy"

    }

)

print("Logistic Regression Accuracy :", lr_accuracy)

-------------------------

dt = DecisionTreeClassifier(

    featuresCol="features",

    labelCol="label",

    predictionCol="prediction",

    maxDepth=8,

    seed=42

)

dt_model = dt.fit(train_df)

dt_predictions = dt_model.transform(test_df)

dt_accuracy = evaluator.evaluate(

    dt_predictions,

    {

        evaluator.metricName: "accuracy"

    }

)

print("Decision Tree Accuracy :", dt_accuracy)

--------------------

rf = RandomForestClassifier(

    featuresCol="features",

    labelCol="label",

    predictionCol="prediction",

    numTrees=100,

    maxDepth=10,

    seed=42

)

rf_model = rf.fit(train_df)

rf_predictions = rf_model.transform(test_df)

rf_accuracy = evaluator.evaluate(

    rf_predictions,

    {

        evaluator.metricName: "accuracy"

    }

)

print("Random Forest Accuracy :", rf_accuracy)

-----------------------

gbt = GBTClassifier(

    featuresCol="features",

    labelCol="label",

    predictionCol="prediction",

    maxDepth=6,

    maxIter=100,

    seed=42

)

gbt_model = gbt.fit(train_df)

gbt_predictions = gbt_model.transform(test_df)

gbt_accuracy = evaluator.evaluate(

    gbt_predictions,

    {

        evaluator.metricName: "accuracy"

    }

)

print("Gradient Boosted Trees Accuracy :", gbt_accuracy)

---------------------------

comparison_df = spark.createDataFrame(

    [

        ("Logistic Regression", float(lr_accuracy)),

        ("Decision Tree", float(dt_accuracy)),

        ("Random Forest", float(rf_accuracy)),

        ("Gradient Boosted Trees", float(gbt_accuracy))

    ],

    [

        "model_name",

        "accuracy"

    ]

)

display(

    comparison_df.orderBy(

        F.desc("accuracy")

    )

)

-----------------

comparison_df = spark.createDataFrame(

    [

        ("Logistic Regression", float(lr_accuracy)),

        ("Decision Tree", float(dt_accuracy)),

        ("Random Forest", float(rf_accuracy))

    ],

    [

        "model_name",

        "accuracy"

    ]

)

display(

    comparison_df.orderBy(

        F.desc("accuracy")

    )

)