# Databricks notebook source
# Optional example only. Keep the notebook outside the default flow in Free Edition.

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(name="sales_orders_clean_dlt")
@dp.expect_or_drop("valid_currency", "currency IN ('EUR', 'USD')")
@dp.expect("non_negative_amount", "amount >= 0")
@dp.expect("customer_is_present", "customer_id IS NOT NULL")
def sales_orders_clean_dlt():
    return spark.read.table("workspace.raw.sales_orders").select(
        "order_id",
        "order_date",
        "customer_id",
        "sku_id",
        F.upper("currency").alias("currency"),
        "amount",
    )
