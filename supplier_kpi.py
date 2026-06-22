For Supplier KPIs, we need to follow your SDD exactly and use your actual Silver schemas.

Relevant Silver Tables
silver_suppliers
supplier_id
supplier_name
contact_person
phone
email
city
country
supplier_rating
last_updated
silver_purchase_orders
purchase_order_id
supplier_id
purchase_order_date
expected_delivery_date
status
silver_purchase_order_items
purchase_order_item_id
purchase_order_id
product_id
quantity
unit_cost
silver_product_suppliers
product_id
supplier_id
KPI Mapping from SDD
KPI	Gold Table
Total Purchase Orders	gold_procurement_metrics
Procurement Volume	gold_procurement_metrics
Procurement Spend	gold_procurement_metrics
Supplier Purchase Volume	gold_supplier_performance
Supplier Spend	gold_supplier_performance
SUPPLIER KPI SECTION (10.5)
Cell 75 — Load Required Tables
from pyspark.sql.functions import *
from pyspark.sql.types import *
silver_suppliers = spark.table(
    "retailmart.silver.silver_suppliers"
)

silver_purchase_orders = spark.table(
    "retailmart.silver.silver_purchase_orders"
)

silver_purchase_order_items = spark.table(
    "retailmart.silver.silver_purchase_order_items"
)

silver_product_suppliers = spark.table(
    "retailmart.silver.silver_product_suppliers"
)
KPI 1
Total Purchase Orders
Cell 76
total_purchase_orders = (

    silver_purchase_orders

    .agg(
        count("purchase_order_id")
        .alias("total_purchase_orders")
    )

)
Cell 77
display(total_purchase_orders)
KPI 2
Procurement Volume

Formula:

SUM(quantity)
Cell 78
procurement_volume = (

    silver_purchase_order_items

    .agg(
        sum("quantity")
        .alias("procurement_volume")
    )

)
Cell 79
display(procurement_volume)
KPI 3
Procurement Spend

Formula:

SUM(quantity * unit_cost)
Cell 80
procurement_spend = (

    silver_purchase_order_items

    .withColumn(
        "spend",
        col("quantity") * col("unit_cost")
    )

    .agg(
        round(
            sum("spend"),
            2
        ).alias("procurement_spend")
    )

)
Cell 81
display(procurement_spend)
Build gold_procurement_metrics
Cell 82
total_po = (

    silver_purchase_orders.count()
)

total_volume = (

    silver_purchase_order_items

    .agg(
        sum("quantity")
    )

    .first()[0]
)

total_spend = (

    silver_purchase_order_items

    .withColumn(
        "spend",
        col("quantity") * col("unit_cost")
    )

    .agg(
        round(
            sum("spend"),
            2
        )
    )

    .first()[0]
)
Cell 83
gold_procurement_metrics = spark.createDataFrame(

    [
        (
            total_po,
            total_volume,
            total_spend
        )
    ],

    [
        "total_purchase_orders",
        "procurement_volume",
        "procurement_spend"
    ]

)
Cell 84
display(
    gold_procurement_metrics
)
Cell 85
gold_procurement_metrics.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable(
    "retailmart.gold.gold_procurement_metrics"
)
KPI 4
Supplier Purchase Volume

Formula from SDD:

SUM(quantity)
GROUP BY supplier_id
KPI 5
Supplier Spend

Formula from SDD:

SUM(quantity * unit_cost)
GROUP BY supplier_id
Build gold_supplier_performance
Cell 86
gold_supplier_performance = (

    silver_purchase_orders.alias("po")

    .join(
        silver_suppliers.alias("s"),
        "supplier_id"
    )

    .join(
        silver_purchase_order_items.alias("poi"),
        "purchase_order_id"
    )

    .withColumn(
        "spend",
        col("quantity") * col("unit_cost")
    )

    .groupBy(

        "supplier_id",
        "supplier_name",
        "city",
        "country",
        "supplier_rating"

    )

    .agg(

        countDistinct(
            "purchase_order_id"
        ).alias(
            "total_purchase_orders"
        ),

        sum("quantity")
        .alias(
            "purchase_volume"
        ),

        round(
            sum("spend"),
            2
        ).alias(
            "supplier_spend"
        )

    )

    .orderBy(
        desc("supplier_spend")
    )

)
Cell 87
display(
    gold_supplier_performance
)
Cell 88
gold_supplier_performance.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable(
    "retailmart.gold.gold_supplier_performance"
)
Optional Supplier Summary

Very useful for Power BI cards.

Cell 89
gold_supplier_summary = (

    silver_suppliers

    .agg(

        count("supplier_id")
        .alias("total_suppliers"),

        round(
            avg("supplier_rating"),
            2
        ).alias("avg_supplier_rating")

    )

)
Cell 90
display(
    gold_supplier_summary
)
Cell 91
gold_supplier_summary.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable(
    "retailmart.gold.gold_supplier_summary"
)
Validation
Cell 92
spark.sql("""
SHOW TABLES IN retailmart.gold
""").show(truncate=False)
Final Gold Tables Created

After completing Sales + Customer + Product + Inventory + Supplier KPIs, you should have:

gold_daily_sales
gold_monthly_sales
gold_store_sales
gold_sales_channel_performance
gold_sales_summary

gold_customer_metrics
gold_customer_purchase_summary
gold_customer_summary

gold_product_performance
gold_top_selling_products
gold_product_category_performance
gold_product_summary

gold_inventory_metrics
gold_low_stock_products
gold_inventory_summary

gold_procurement_metrics
gold_supplier_performance
gold_supplier_summary

This fully covers Sections 10.1 to 10.5 of your SDD and gives Power BI ready Gold-layer datasets.