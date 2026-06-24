gold_supplier_dependency = (

    silver_product_suppliers.alias("ps")

    .join(
        silver_suppliers.alias("s"),
        "supplier_id"
    )

    .groupBy(
        "supplier_id",
        "supplier_name",
        "supplier_rating",
        "city",
        "country"
    )

    .agg(

        countDistinct("product_id")
        .alias("products_supplied")

    )

    .orderBy(
        desc("products_supplied")
    )

)


display(gold_supplier_dependency)


gold_supplier_dependency.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable(
    "retailmart.gold.gold_supplier_dependency"
)

gold_supplier_risk_products = (

    silver_product_suppliers

    .groupBy("product_id")

    .agg(

        countDistinct("supplier_id")
        .alias("supplier_count")

    )

    .filter(
        col("supplier_count") == 1
    )

)

display(gold_supplier_risk_products)

gold_supplier_risk_products.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable(
    "retailmart.gold.gold_supplier_risk_products"
)


redundant tables:

- gold_customer_summary 
- gold_product_summary 
- gold_supplier_summary 
- gold_procurement_metrics 
- gold_customer_metrics 
- gold_inventory_summary 
- gold_sales_summary