For Inventory KPIs, your SDD specifies:

KPI	Gold Table
Current Stock	gold_inventory_metrics
Inventory by Store	gold_inventory_metrics
Inventory by Product	gold_inventory_metrics
Store Coverage	gold_inventory_metrics
Low Stock Products	gold_low_stock_products

And your actual Silver schemas are:

silver_inventory
inventory_id
product_id
store_id
stock_quantity
reorder_level
safety_stock
last_updated
silver_products
product_id
product_name
category
subcategory
brand
...
silver_stores
store_id
store_name
city
state
region
...
INVENTORY KPI SECTION (10.4)
Cell 55 — Load Required Tables
from pyspark.sql.functions import *
from pyspark.sql.types import *
silver_inventory = spark.table(
    "retailmart.silver.silver_inventory"
)

silver_products = spark.table(
    "retailmart.silver.silver_products"
)

silver_stores = spark.table(
    "retailmart.silver.silver_stores"
)
KPI 1
Current Stock

Total stock available across all stores.

Cell 56
current_stock = (

    silver_inventory

    .agg(
        sum("stock_quantity")
        .alias("current_stock")
    )

)
Cell 57
display(current_stock)
KPI 2
Inventory By Store
Cell 58
inventory_by_store = (

    silver_inventory.alias("i")

    .join(
        silver_stores.alias("s"),
        "store_id"
    )

    .groupBy(
        "store_id",
        "store_name",
        "city",
        "state",
        "region"
    )

    .agg(
        sum("stock_quantity")
        .alias("store_stock")
    )

    .orderBy(
        desc("store_stock")
    )

)
Cell 59
display(inventory_by_store)
KPI 3
Inventory By Product
Cell 60
inventory_by_product = (

    silver_inventory.alias("i")

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
        sum("stock_quantity")
        .alias("product_stock")
    )

    .orderBy(
        desc("product_stock")
    )

)
Cell 61
display(inventory_by_product)
KPI 4
Store Coverage

Business Definition:

How many stores stock a product
Cell 62
store_coverage = (

    silver_inventory

    .filter(
        col("stock_quantity") > 0
    )

    .groupBy(
        "product_id"
    )

    .agg(
        countDistinct("store_id")
        .alias("store_coverage")
    )

)
Cell 63
store_coverage = (

    store_coverage

    .join(
        silver_products.select(
            "product_id",
            "product_name",
            "category"
        ),
        "product_id"
    )

    .select(
        "product_id",
        "product_name",
        "category",
        "store_coverage"
    )

    .orderBy(
        desc("store_coverage")
    )

)
Cell 64
display(store_coverage)
Build gold_inventory_metrics

Instead of creating separate tables for every KPI, combine them into one analytical table.

Cell 65
gold_inventory_metrics = (

    silver_inventory.alias("i")

    .join(
        silver_products.alias("p"),
        "product_id"
    )

    .join(
        silver_stores.alias("s"),
        "store_id"
    )

    .groupBy(

        "product_id",
        "product_name",
        "category",

        "store_id",
        "store_name",
        "city",
        "state"

    )

    .agg(

        sum("stock_quantity")
        .alias("current_stock"),

        first("reorder_level")
        .alias("reorder_level"),

        first("safety_stock")
        .alias("safety_stock")

    )

)
Cell 66
gold_inventory_metrics = (

    gold_inventory_metrics

    .join(
        store_coverage.select(
            "product_id",
            "store_coverage"
        ),
        "product_id",
        "left"
    )

)
Cell 67
display(gold_inventory_metrics)
Cell 68
gold_inventory_metrics.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable(
    "retailmart.gold.gold_inventory_metrics"
)
KPI 5
Low Stock Products

SDD:

stock_quantity < threshold

Since your Silver layer already enforces:

safety_stock <= reorder_level <= stock_quantity

a practical inventory threshold is:

stock_quantity <= reorder_level

This identifies products that need replenishment.

Cell 69
gold_low_stock_products = (

    silver_inventory.alias("i")

    .join(
        silver_products.alias("p"),
        "product_id"
    )

    .join(
        silver_stores.alias("s"),
        "store_id"
    )

    .filter(
        col("stock_quantity")
        <=
        col("reorder_level")
    )

    .select(

        "inventory_id",

        "product_id",
        "product_name",
        "category",

        "store_id",
        "store_name",

        "stock_quantity",
        "reorder_level",
        "safety_stock",

        "last_updated"

    )

    .orderBy(
        "stock_quantity"
    )

)
Cell 70
display(
    gold_low_stock_products
)
Cell 71
gold_low_stock_products.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable(
    "retailmart.gold.gold_low_stock_products"
)
Optional Dashboard Summary

Useful for KPI cards.

Cell 72
gold_inventory_summary = (

    silver_inventory

    .agg(

        sum("stock_quantity")
        .alias("total_stock"),

        countDistinct("product_id")
        .alias("total_products"),

        countDistinct("store_id")
        .alias("total_stores")

    )

)
Cell 73
display(
    gold_inventory_summary
)
Cell 74
gold_inventory_summary.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable(
    "retailmart.gold.gold_inventory_summary"
)
Validation
spark.sql("""
SHOW TABLES IN retailmart.gold
""").show(truncate=False)
Tables created
gold_inventory_metrics
gold_low_stock_products
gold_inventory_summary

These fully implement the Inventory KPI section from your SDD and are ready for Power BI visualizations like:

Total Stock Card
Inventory by Store Bar Chart
Inventory by Product Bar Chart
Store Coverage Heatmap
Low Stock Products Table

The final domain remaining after this is 10.5 Supplier KPIs, which will use:

silver_suppliers
silver_purchase_orders
silver_purchase_order_items
silver_product_suppliers

to build:

gold_procurement_metrics
gold_supplier_performance

exactly matching your SDD.