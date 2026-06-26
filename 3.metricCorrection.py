CREATE OR REPLACE VIEW retailmart.gold_top10_store_revenue AS
SELECT *
FROM retailmart.gold_store_sales
ORDER BY store_revenue DESC
LIMIT 10;


CREATE OR REPLACE VIEW retailmart.gold_bottom10_store_revenue AS
SELECT *
FROM retailmart.gold_store_sales
ORDER BY store_revenue ASC
LIMIT 10;


from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Read existing Gold table
df = spark.table("retailmart.gold_store_sales")

# Window for ranking
rank_window = Window.orderBy(F.desc("store_revenue"))

# Total revenue
total_revenue = df.agg(
    F.sum("store_revenue").alias("total")
).collect()[0]["total"]

# Add columns
df_new = (
    df
    .withColumn(
        "revenue_rank",
        F.rank().over(rank_window)
    )
    .withColumn(
        "revenue_contribution_pct",
        F.round(
            (F.col("store_revenue") / F.lit(total_revenue)) * 100,
            2
        )
    )
)

# Overwrite Gold table
(
    df_new.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("retailmart.gold.gold_store_sales")
)