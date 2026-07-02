from pyspark.sql import functions as F
from pyspark.sql.functions import *

from pyspark.sql.types import *

from pyspark.ml.feature import StringIndexer

from pyspark.ml import Pipeline

----------------

CATALOG = "retailmart"

SCHEMA = "silver"

OUTPUT_SCHEMA = "ml_db"

INVENTORY_TABLE = f"{CATALOG}.{SCHEMA}.silver_inventory"

PRODUCT_TABLE = f"{CATALOG}.{SCHEMA}.silver_products"

STORE_TABLE = f"{CATALOG}.{SCHEMA}.silver_stores"

OUTPUT_TABLE = f"{CATALOG}.{OUTPUT_SCHEMA}.inventory_risk_features"

------------

inventory_df = spark.table(INVENTORY_TABLE)

products_df = spark.table(PRODUCT_TABLE)

stores_df = spark.table(STORE_TABLE)

print("Inventory :", inventory_df.count())

print("Products  :", products_df.count())

print("Stores    :", stores_df.count())

--------------

feature_df = (

    inventory_df.alias("i")

    .join(

        products_df.alias("p"),

        "product_id",

        "left"

    )

    .join(

        stores_df.alias("s"),

        "store_id",

        "left"

    )

)

-------------------

feature_df = feature_df.select(

    "inventory_id",

    "product_id",

    "store_id",

    "stock_quantity",

    "reorder_level",

    "safety_stock",

    "last_updated",

    "product_name",

    "category",

    "brand",

    "store_name",

    "city",

    "state"

)

-------------------

feature_df = (

    feature_df

    .withColumn(

        "stock_gap",

        col("stock_quantity") - col("reorder_level")

    )

    .withColumn(

        "stock_to_reorder_ratio",

        round(

            col("stock_quantity") /

            when(col("reorder_level")==0,1)

            .otherwise(col("reorder_level")),

            2

        )

    )

    .withColumn(

        "inventory_buffer",

        col("stock_quantity")

        -

        col("reorder_level")

        -

        col("safety_stock")

    )

    .withColumn(

        "safety_stock_ratio",

        round(

            col("stock_quantity")

            /

            when(col("safety_stock")==0,1)

            .otherwise(col("safety_stock")),

            2

        )

    )

)

---------------------

feature_df = feature_df.withColumn(

    "days_since_update",

    datediff(

        current_date(),

        col("last_updated")

    )

)

--------------------

feature_df = (

    feature_df

    .withColumn(

        "inventory_risk",

        when(

            col("stock_quantity")

            <=

            col("reorder_level"),

            "High Risk"

        )

        .when(

            col("stock_quantity")

            <=

            col("reorder_level")

            +

            col("safety_stock"),

            "Medium Risk"

        )

        .otherwise(

            "Low Risk"

        )

    )

)

---------------------------

indexers = [

    StringIndexer(

        inputCol="category",

        outputCol="category_index",

        handleInvalid="keep"

    ),

    StringIndexer(

        inputCol="brand",

        outputCol="brand_index",

        handleInvalid="keep"

    ),

    StringIndexer(

        inputCol="city",

        outputCol="city_index",

        handleInvalid="keep"

    ),

    StringIndexer(

        inputCol="state",

        outputCol="state_index",

        handleInvalid="keep"

    ),

    StringIndexer(

        inputCol="inventory_risk",

        outputCol="label",

        handleInvalid="keep"

    )

]

-----------------------

pipeline = Pipeline(

    stages=indexers

)

pipeline_model = pipeline.fit(

    feature_df

)

feature_df = pipeline_model.transform(

    feature_df

)

--------------------------

feature_df = feature_df.select(

    "inventory_id",

    "product_id",

    "store_id",

    "stock_quantity",

    "reorder_level",

    "safety_stock",

    "stock_gap",

    "stock_to_reorder_ratio",

    "inventory_buffer",

    "safety_stock_ratio",

    "days_since_update",

    "category_index",

    "brand_index",

    "city_index",

    "state_index",

    "inventory_risk",

    "label"

)

-----------------------

(

    feature_df.write

    .mode("overwrite")

    .format("delta")

    .saveAsTable(

        OUTPUT_TABLE

    )

)

---------------------

print("="*70)

print("Inventory Risk Feature Engineering Completed")

print()

print("Total Records :", feature_df.count())

print()

print("Output Table :", OUTPUT_TABLE)

print("="*70)

display(feature_df.limit(20))

----------------------

