# Databricks notebook source
# MAGIC %run ../_common/00_setup

# COMMAND ----------

config = load_config_from_widgets(dbutils)
paths = ensure_base_objects(spark, config)

dbutils.widgets.text("INJECT_DEFECTS", "true")
inject_defects = dbutils.widgets.get("INJECT_DEFECTS").lower() == "true"
version_tag = "sales_bad_v1" if inject_defects else "sales_fixed_v2"

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
        "amount": -50.0 if inject_defects else 50.0,
    },
    {
        "order_id": "SO-003",
        "order_date": "2024-13-19" if inject_defects else "2024-01-19",
        "customer_id": "CUST-03",
        "sku_id": "SKU-300",
        "currency": "USD",
        "amount": 300.0,
    },
    {
        "order_id": "SO-004",
        "order_date": "2024-01-20",
        "customer_id": None if inject_defects else "CUST-04",
        "sku_id": "SKU-400",
        "currency": "EUR",
        "amount": 89.9,
    },
    {
        "order_id": "SO-005",
        "order_date": "2024-01-21",
        "customer_id": "CUST-05",
        "sku_id": "SKU-500",
        "currency": "BTC" if inject_defects else "USD",
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

inventory_records = [
    {
        "movement_id": "IM-001",
        "movement_date": "2024-01-15",
        "warehouse_id": "WH-01",
        "sku_id": "SKU-100",
        "quantity_delta": 12,
        "movement_type": "IN",
    },
    {
        "movement_id": "IM-002",
        "movement_date": "2024-01-16",
        "warehouse_id": "WH-01",
        "sku_id": "SKU-200",
        "quantity_delta": -3,
        "movement_type": "OUT",
    },
    {
        "movement_id": "IM-003",
        "movement_date": "2024-01-20",
        "warehouse_id": "WH-02",
        "sku_id": "SKU-500",
        "quantity_delta": 10,
        "movement_type": "IN",
    },
]

sales_df = spark.createDataFrame(sales_records)
inventory_df = spark.createDataFrame(inventory_records)

sales_path = f"{paths['landing_root']}/raw/sales_orders"
inventory_path = f"{paths['landing_root']}/raw/inventory_moves"

overwrite_json_dataset(sales_df, sales_path)
overwrite_json_dataset(inventory_df, inventory_path)

write_seed_records(
    spark,
    config,
    dataset_name="sales_orders",
    records=sales_records,
    injected_defects=inject_defects,
    version_tag=version_tag,
)
write_seed_records(
    spark,
    config,
    dataset_name="inventory_moves",
    records=inventory_records,
    injected_defects=False,
    version_tag="inventory_v1",
)

summary_df = spark.createDataFrame(
    [
        ("sales_orders", sales_path, inject_defects, version_tag),
        ("inventory_moves", inventory_path, False, "inventory_v1"),
    ],
    "dataset_name string, landing_path string, injected_defects boolean, version_tag string",
)

safe_display(summary_df)
