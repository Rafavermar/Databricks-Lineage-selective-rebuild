# Databricks notebook source
# MAGIC %run ../_common/00_setup

# COMMAND ----------

config = load_config_from_widgets(dbutils)
paths = ensure_base_objects(spark, config)

sales_records = [
    {
        "order_id": "SO-001",
        "order_date": "2024-01-15",
        "customer_id": "CUST-01",
        "sku_id": "SKU-100",
        "currency": "EUR",
        "amount": 120.5,
    },
    {
        "order_id": "SO-002",
        "order_date": "2024-01-16",
        "customer_id": "CUST-02",
        "sku_id": "SKU-200",
        "currency": "EUR",
        "amount": 50.0,
    },
    {
        "order_id": "SO-003",
        "order_date": "2024-01-19",
        "customer_id": "CUST-03",
        "sku_id": "SKU-300",
        "currency": "USD",
        "amount": 300.0,
    },
    {
        "order_id": "SO-004",
        "order_date": "2024-01-20",
        "customer_id": "CUST-04",
        "sku_id": "SKU-400",
        "currency": "EUR",
        "amount": 89.9,
    },
    {
        "order_id": "SO-005",
        "order_date": "2024-01-21",
        "customer_id": "CUST-05",
        "sku_id": "SKU-500",
        "currency": "USD",
        "amount": 215.0,
    },
    {
        "order_id": "SO-006",
        "order_date": "2024-01-22",
        "customer_id": "CUST-06",
        "sku_id": "SKU-600",
        "currency": "USD",
        "amount": 149.0,
    },
]

sales_df = spark.createDataFrame(sales_records)
sales_path = f"{paths['landing_root']}/raw/sales_orders"
overwrite_json_dataset(sales_df, sales_path)
write_seed_records(
    spark,
    config,
    dataset_name="sales_orders",
    records=sales_records,
    injected_defects=False,
    version_tag="sales_fixed_v2",
)

fix_df = spark.createDataFrame(
    [("sales_orders", sales_path, "clean overwrite applied")],
    "dataset_name string, landing_path string, status string",
)

safe_display(fix_df)
