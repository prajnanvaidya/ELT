# ==============================================
# NOTEBOOK 02
# CUSTOMER CLUSTERING - MODEL TRAINING
# ==============================================

-----------------------

from pyspark.ml.clustering import KMeans

from pyspark.ml.evaluation import ClusteringEvaluator

from pyspark.sql import functions as F

-----------------------

DATABASE = "retailmart.ml_db"

FEATURE_TABLE = f"{DATABASE}.customer_cluster_features"

------------------------

customer_features = spark.table(FEATURE_TABLE)

--------------------------

print("Customer Feature Table")

customer_features.printSchema()

print()

print("Total Customers")

print(customer_features.count())

--------------------------

display(customer_features)

------------------

k_values = [2,3,4,5,6]

results = []

models = {}

------------------------

for k in k_values:

    print(f"Training KMeans (K = {k})")

    kmeans = KMeans(

        featuresCol="features",

        predictionCol="cluster",

        k=k,

        seed=42

    )

    model = kmeans.fit(customer_features)

    prediction = model.transform(customer_features)

    evaluator = ClusteringEvaluator(

        featuresCol="features",

        predictionCol="cluster",

        metricName="silhouette"

    )

    score = evaluator.evaluate(prediction)

    results.append((k, score))

    models[k] = model

    print(f"Silhouette Score : {score}")

    print()

----------------------------------

comparison_df = spark.createDataFrame(

    results,

    [

        "k",

        "silhouette_score"

    ]

)

------------------------

display(

    comparison_df.orderBy(

        F.desc("silhouette_score")

    )

)

------------------

best_row = comparison_df.orderBy(

    F.desc("silhouette_score")

).first()

BEST_K = best_row["k"]

BEST_SCORE = best_row["silhouette_score"]

print("Best K")

print(BEST_K)

print()

print("Best Silhouette Score")

print(BEST_SCORE)

-----------------------------

best_model = models[BEST_K]

----------------------

cluster_prediction = best_model.transform(

    customer_features

)

------------------------

display(cluster_prediction)

---------------------

cluster_prediction.groupBy(

    "cluster"

).count().orderBy(

    "cluster"

).display()

--------------------

centers = best_model.clusterCenters()

for i, center in enumerate(centers):

    print()

    print(f"Cluster {i}")

    print(center)

---------------------

cluster_prediction.groupBy(

    "cluster"

).agg(

    F.round(

        F.avg("total_spend"),

        2

    ).alias("avg_spend"),

    F.round(

        F.avg("total_orders"),

        2

    ).alias("avg_orders"),

    F.round(

        F.avg("purchase_frequency"),

        2

    ).alias("avg_frequency"),

    F.round(

        F.avg("avg_customer_spend"),

        2

    ).alias("avg_customer_spend"),

    F.count("*").alias("customers")

).orderBy(

    "cluster"

).display()

--------------------------

