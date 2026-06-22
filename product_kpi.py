For Product KPIs, we must strictly follow your SDD and your actual Silver schemas.

Relevant Silver Tables
silver_products
-------------
product_id
product_name
category
subcategory
brand
sku
unit_cost
unit_price
product_rating
last_updated
silver_order_items
------------------
order_item_id
order_id
product_id
quantity
item_amount
PRODUCT KPI SECTION (10.3)

Create these Gold tables:

gold_product_performance
gold_top_selling_products
gold_product_category_performance
Cell 40 — Load Required Tables
silver_products = spark.table(
    "retailmart.silver.silver_products"
)

silver_order_items = spark.table(
    "retailmart.silver.silver_order_items"
)
KPI 1 + KPI 2
Product Revenue
Product Sales Quantity

Gold Table:

gold_product_performance
Cell 41
gold_product_performance = (

    silver_order_items.alias("oi")

    .join(
        silver_products.alias("p"),
        "product_id"
    )

    .groupBy(
        "product_id",
        "product_name",
        "category",
        "subcategory",
        "brand"
    )

    .agg(

        round(
            sum("item_amount"),
            2
        ).alias("product_revenue"),

        sum("quantity")
        .alias("product_sales_qty")

    )

    .orderBy(
        desc("product_revenue")
    )

)
Cell 42
display(gold_product_performance)
Cell 43
gold_product_performance.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable(
    "retailmart.gold.gold_product_performance"
)
KPI 3
Top Selling Products

Business Rule:

Rank products by quantity sold DESC
Cell 44
from pyspark.sql.window import Window
Cell 45
ranking_window = Window.orderBy(
    desc("product_sales_qty")
)
Cell 46
gold_top_selling_products = (

    gold_product_performance

    .withColumn(
        "sales_rank",
        dense_rank().over(
            ranking_window
        )
    )

    .orderBy(
        "sales_rank"
    )

)
Cell 47
display(
    gold_top_selling_products
)
Cell 48
gold_top_selling_products.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable(
    "retailmart.gold.gold_top_selling_products"
)
KPI 4 + KPI 5
Category Revenue
Category Sales Quantity

Gold Table:

gold_product_category_performance
Cell 49
gold_product_category_performance = (

    silver_order_items.alias("oi")

    .join(
        silver_products.alias("p"),
        "product_id"
    )

    .groupBy(
        "category"
    )

    .agg(

        round(
            sum("item_amount"),
            2
        ).alias("category_revenue"),

        sum("quantity")
        .alias("category_sales_qty")

    )

    .orderBy(
        desc("category_revenue")
    )

)
Cell 50
display(
    gold_product_category_performance
)
Cell 51
gold_product_category_performance.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable(
    "retailmart.gold.gold_product_category_performance"
)
Optional Product Dashboard Summary (Recommended)

Very useful for Power BI KPI cards.

Cell 52
gold_product_summary = (

    silver_order_items

    .agg(

        round(
            sum("item_amount"),
            2
        ).alias("total_product_revenue"),

        sum("quantity")
        .alias("total_quantity_sold"),

        countDistinct("product_id")
        .alias("products_sold")

    )

)
Cell 53
display(gold_product_summary)
Cell 54
gold_product_summary.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable(
    "retailmart.gold.gold_product_summary"
)
Validation
spark.sql("""
SHOW TABLES IN retailmart.gold
""").show(truncate=False)
Tables created after Product KPI notebook
gold_product_performance
gold_top_selling_products
gold_product_category_performance
gold_product_summary

These match the KPI definitions in your SDD:

Product Revenue ✅
Product Sales Qty ✅
Top Selling Products ✅
Category Revenue ✅
Category Sales Qty ✅

Next, move to 10.4 Inventory KPIs, which will use:

silver_inventory
silver_products
silver_stores

to create:

gold_inventory_metrics
gold_low_stock_products