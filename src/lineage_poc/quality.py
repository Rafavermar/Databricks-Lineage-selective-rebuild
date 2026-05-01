from __future__ import annotations

import json
from datetime import datetime
from typing import List

from pyspark.sql import SparkSession, functions as F

from lineage_poc.config import PocConfig


def evaluate_sales_gold_quality(spark: SparkSession, config: PocConfig) -> None:
    sales_gold = spark.table(config.uc_table("gold", "v_sales_summary"))
    invalid_source_rows = spark.table(config.uc_table("stage", "sales_orders_clean")).where(
        ~F.col("valid_record_flag")
    )

    metric_row = sales_gold.agg(
        F.sum("orders_total").alias("orders_total"),
        F.sum("invalid_order_count").alias("invalid_order_count"),
        F.sum("negative_amount_count").alias("negative_amount_count"),
        F.sum("null_customer_count").alias("null_customer_count"),
        F.sum("invalid_currency_count").alias("invalid_currency_count"),
        F.sum("invalid_date_count").alias("invalid_date_count"),
    ).collect()[0]

    total_count = int(metric_row["orders_total"] or 0)
    sample_rows = [row.asDict() for row in invalid_source_rows.limit(5).collect()]
    sample_json = json.dumps(sample_rows, default=str)

    checks = [
        ("invalid_order_count_is_zero", int(metric_row["invalid_order_count"] or 0)),
        ("negative_amount_count_is_zero", int(metric_row["negative_amount_count"] or 0)),
        ("null_customer_count_is_zero", int(metric_row["null_customer_count"] or 0)),
        ("invalid_currency_count_is_zero", int(metric_row["invalid_currency_count"] or 0)),
        ("invalid_date_count_is_zero", int(metric_row["invalid_date_count"] or 0)),
    ]

    rows: List[tuple] = []
    run_id = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    for check_name, invalid_count in checks:
        invalid_pct = (invalid_count / total_count) if total_count else 0.0
        status = "PASS" if invalid_count == 0 else "FAIL"
        rows.append(
            (
                datetime.utcnow(),
                run_id,
                config.uc_table("gold", "v_sales_summary"),
                check_name,
                invalid_count,
                total_count,
                invalid_pct,
                status,
                sample_json if invalid_count else "[]",
            )
        )

    quality_df = spark.createDataFrame(
        rows,
        "event_time timestamp, run_id string, target_full string, check_name string, "
        "invalid_count bigint, total_count bigint, invalid_pct double, status string, "
        "sample_json string",
    )
    quality_df.write.mode("append").saveAsTable(config.audit_table("quality_results"))
