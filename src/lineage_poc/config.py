from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional


POC_SCHEMAS: List[str] = ["raw", "stage", "core", "curated", "gold", "audit"]

LAYER_DEFINITIONS: List[Dict[str, object]] = [
    {"schema_name": "raw", "layer_alias": "raw", "layer_rank": 2},
    {"schema_name": "stage", "layer_alias": "stage", "layer_rank": 3},
    {"schema_name": "core", "layer_alias": "core", "layer_rank": 4},
    {"schema_name": "curated", "layer_alias": "curated", "layer_rank": 5},
    {"schema_name": "gold", "layer_alias": "gold", "layer_rank": 6},
]

DEFAULT_CATALOG_NAME = "workspace"
DEFAULT_VOLUME_NAME = "poc_volume"
DEFAULT_MAX_HOPS = 6
DEFAULT_NOTEBOOK_BASE = "notebooks"
DEFAULT_TARGET_TABLE_NAME = "gold.v_sales_summary"
DEFAULT_DATE_START = "2024-01-01"
DEFAULT_DATE_END = "2024-12-31"
AUDIT_SCHEMA = "audit"


@dataclass(frozen=True)
class PocConfig:
    catalog_name: str = DEFAULT_CATALOG_NAME
    volume_name: str = DEFAULT_VOLUME_NAME
    target_table_name: str = DEFAULT_TARGET_TABLE_NAME
    max_hops: int = DEFAULT_MAX_HOPS
    notebook_base: str = DEFAULT_NOTEBOOK_BASE
    test_ranks: str = ""
    test_limit_per_rank: int = 0
    ingest_mode: str = "auto"

    def audit_table(self, table_name: str) -> str:
        return f"{self.catalog_name}.{AUDIT_SCHEMA}.{table_name}"

    def uc_table(self, schema_name: str, table_name: str) -> str:
        return f"{self.catalog_name}.{schema_name}.{table_name}"

    @property
    def normalized_target(self) -> str:
        return normalize_table_name(self.target_table_name, self.catalog_name)

    @property
    def volume_full_name(self) -> str:
        return f"{self.catalog_name}.{AUDIT_SCHEMA}.{self.volume_name}"


def normalize_table_name(table_name: str, catalog_name: str = DEFAULT_CATALOG_NAME) -> str:
    parts = [part.strip() for part in table_name.split(".") if part.strip()]
    if len(parts) == 3:
        return ".".join(parts)
    if len(parts) == 2:
        return f"{catalog_name}.{parts[0]}.{parts[1]}"
    raise ValueError(f"Unsupported table name format: {table_name}")


def split_table_name(table_name: str, catalog_name: str = DEFAULT_CATALOG_NAME) -> List[str]:
    return normalize_table_name(table_name, catalog_name).split(".")


def build_poc_config(overrides: Optional[Dict[str, object]] = None) -> PocConfig:
    overrides = overrides or {}
    max_hops = int(overrides.get("max_hops", DEFAULT_MAX_HOPS))
    test_limit = int(overrides.get("test_limit_per_rank", 0) or 0)
    return PocConfig(
        catalog_name=str(overrides.get("catalog_name", DEFAULT_CATALOG_NAME)),
        volume_name=str(overrides.get("volume_name", DEFAULT_VOLUME_NAME)),
        target_table_name=str(
            overrides.get("target_table_name", DEFAULT_TARGET_TABLE_NAME)
        ),
        max_hops=max_hops,
        notebook_base=str(overrides.get("notebook_base", DEFAULT_NOTEBOOK_BASE)),
        test_ranks=str(overrides.get("test_ranks", "")),
        test_limit_per_rank=test_limit,
        ingest_mode=str(overrides.get("ingest_mode", "auto")),
    )
