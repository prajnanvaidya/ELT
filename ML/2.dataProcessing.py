Notebook 02 – Data Preprocessing
Cell 1 (Markdown)
# RetailMart ML

## Notebook 02 - Data Preprocessing

### Objective

Prepare the engineered customer feature table for machine learning.

This notebook performs:

- Load ML feature table
- Remove unnecessary identifier columns
- Encode categorical features
- Assemble feature vector
- Train/Test Split
- Save processed datasets
Cell 2
from pyspark.sql import functions as F

from pyspark.ml import Pipeline

from pyspark.ml.feature import (
    StringIndexer,
    OneHotEncoder,
    VectorAssembler
)
Cell 3
CATALOG = "retailmart"

ML_SCHEMA = "ml_db"
Cell 4
customer_df = spark.table(
    f"{CATALOG}.{ML_SCHEMA}.customer_spend_ml_features"
)
Cell 5
display(customer_df)
Cell 6

Drop columns that should never be used for learning.

customer_df = customer_df.drop(
    "customer_name",
    "email",
    "phone",
    "registration_date"
)

Notice:

Do NOT drop customer_id.

We need it later for predictions.

Cell 7

Define categorical columns.

categorical_columns = [

    "city",

    "state",

    "loyalty_level",

    "preferred_payment_method",

    "preferred_sales_channel"

]
Cell 8

Define numerical columns.

numerical_columns = [

    "customer_tenure_days",

    "historical_total_orders",

    "historical_total_spend",

    "historical_avg_order_value",

    "historical_max_order",

    "historical_min_order"

]
Cell 9

Target column.

label_column = "future_customer_spend"
Cell 10

Create String Indexers.

indexers = [

    StringIndexer(

        inputCol=column,

        outputCol=f"{column}_index",

        handleInvalid="keep"

    )

    for column in categorical_columns

]
Cell 11

One Hot Encoder.

encoders = [

    OneHotEncoder(

        inputCol=f"{column}_index",

        outputCol=f"{column}_encoded"

    )

    for column in categorical_columns

]
Cell 12

Vector Assembler.

assembler = VectorAssembler(

    inputCols=

        numerical_columns +

        [

            f"{column}_encoded"

            for column in categorical_columns

        ],

    outputCol="features"

)
Cell 13

Create Pipeline.

pipeline = Pipeline(

    stages=

        indexers +

        encoders +

        [assembler]

)
Cell 14

Fit Pipeline.

preprocessing_model = pipeline.fit(customer_df)

processed_df = preprocessing_model.transform(customer_df)
Cell 15

Prepare final dataset.

processed_df = processed_df.select(

    "customer_id",

    "features",

    F.col(label_column).alias("label")

)
Cell 16

Train Test Split.

train_df, test_df = processed_df.randomSplit(

    [0.8, 0.2],

    seed=42

)
Cell 17

Display.

print("Training Records :", train_df.count())

print("Testing Records :", test_df.count())
Cell 18

Preview.

display(train_df)

display(test_df)
Cell 19

Save Train Dataset.

(
    train_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        f"{CATALOG}.{ML_SCHEMA}.customer_spend_train"
    )
)
Cell 20

Save Test Dataset.

(
    test_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        f"{CATALOG}.{ML_SCHEMA}.customer_spend_test"
    )
)
Cell 21

Verify.

display(
    spark.table(
        f"{CATALOG}.{ML_SCHEMA}.customer_spend_train"
    )
)

display(
    spark.table(
        f"{CATALOG}.{ML_SCHEMA}.customer_spend_test"
    )
)


cell 15:
(
    preprocessing_model
    .write()
    .overwrite()
    .save(
        f"/Volumes/{CATALOG}/{ML_SCHEMA}/preprocessing_pipeline"
    )
)


