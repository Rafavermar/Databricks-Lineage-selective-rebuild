from __future__ import annotations

import re
from typing import Optional, Tuple

from lineage_poc.config import DEFAULT_NOTEBOOK_BASE, split_table_name


NUMERIC_SUFFIX_PATTERN = re.compile(r"(?:_)?\d{6,}$")


def infer_relative_notebook_path(
    full_name: str,
    notebook_base: str = DEFAULT_NOTEBOOK_BASE,
) -> Optional[str]:
    _, schema_name, table_name = split_table_name(full_name)
    if NUMERIC_SUFFIX_PATTERN.search(table_name):
        return None

    if schema_name == "gold":
        return f"{notebook_base}/gold/dml/consumer/{table_name}"
    if schema_name == "curated":
        return f"{notebook_base}/curated/dml/{_curated_folder(table_name)}/{table_name}"
    if schema_name in {"raw", "stage", "core"}:
        product_name, module_name = _product_module_for(schema_name, table_name)
        return f"{notebook_base}/{schema_name}/dml/{product_name}/{module_name}/{table_name}"
    return None


def infer_workspace_notebook_path(
    full_name: str,
    notebook_base: str = DEFAULT_NOTEBOOK_BASE,
    workspace_file_path: str = "${workspace.file_path}",
) -> Optional[str]:
    relative_path = infer_relative_notebook_path(full_name, notebook_base)
    if relative_path is None:
        return None
    return f"{workspace_file_path}/{relative_path}"


def should_exclude_numeric_suffix(full_name: str) -> bool:
    _, _, table_name = split_table_name(full_name)
    return bool(NUMERIC_SUFFIX_PATTERN.search(table_name))


def _curated_folder(table_name: str) -> str:
    if table_name.startswith("dim"):
        return "dim"
    if table_name.startswith("fact"):
        return "fact"
    if table_name.startswith("aux"):
        return "auxiliary"
    if table_name.startswith("helper"):
        return "helper"
    return "kpi"


def _product_module_for(schema_name: str, table_name: str) -> Tuple[str, str]:
    normalized = table_name.lower()
    if normalized == "dim_date":
        return "SHARED", "CALENDAR"
    if normalized == "fact_sales":
        return "SALES", "FACT"
    if normalized == "fact_inventory":
        return "INVENTORY", "FACT"

    tokens = [token for token in normalized.split("_") if token]
    if not tokens:
        return "COMMON", "GENERAL"

    if tokens[0] == "fact" and len(tokens) > 1:
        return tokens[1].upper(), "FACT"
    if tokens[0] == "dim" and len(tokens) > 1:
        return "SHARED", tokens[1].upper()

    product = tokens[0].upper()
    module = tokens[1].upper() if len(tokens) > 1 else "GENERAL"

    if schema_name == "core" and module == "DATE":
        return "SHARED", "CALENDAR"
    return product, module
