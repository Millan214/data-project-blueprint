"""All runtime configuration lives here.

Edit the defaults below to change behaviour. There is no `.env`, no YAML, no
external config — one file, one source of truth.

Flat fields:           `settings.orders_table`, `settings.storage_root`
Nested catalog:        `settings.bronze.orders.table`, `settings.gold.orders_daily.format`
"""

from __future__ import annotations

from pydantic import BaseModel, Field, RootModel


class TableSpec(BaseModel):
    """Physical layout for one table."""

    layer: str
    table: str
    format: str = "parquet"


class TableGroup(RootModel[dict[str, TableSpec]]):
    """Layer's table catalog with attribute-style access (`group.table_name`)."""

    def __getattr__(self, name: str) -> TableSpec:
        if name.startswith("_") or name == "root":
            raise AttributeError(name)
        try:
            return self.root[name]
        except KeyError as exc:
            raise AttributeError(
                f"no table {name!r} in group; available: {list(self.root)}"
            ) from exc

    def __iter__(self):  # type: ignore[override]
        return iter(self.root)

    def __contains__(self, name: object) -> bool:
        return name in self.root


class Settings(BaseModel):
    """Edit the defaults below to reconfigure the pipeline."""

    # --- Environment ---------------------------------------------------------
    env: str = "dev"
    storage_root: str = "file://./data"
    log_level: str = "INFO"
    quarantine_threshold: float = Field(0.20, ge=0.0, le=1.0)

    # --- Medallion layer directory names ------------------------------------
    landing_dir: str = "00_landing"
    bronze_dir: str = "01_bronze"
    silver_dir: str = "02_silver"
    gold_dir: str = "03_gold"
    logs_dir: str = "_logs"

    # --- Orders pipeline -----------------------------------------------------
    orders_landing_file: str = "orders_raw.csv"
    orders_table: str = "orders"
    orders_daily_table: str = "orders_daily"
    orders_dedupe_keys: list[str] = ["order_id"]

    # --- Business validation rules ------------------------------------------
    allowed_currencies: list[str] = ["EUR", "USD", "GBP"]
    customer_id_pattern: str = r"^C\d{3,}$"

    # --- Catalog (nested attribute access) ----------------------------------
    bronze: TableGroup = TableGroup(
        {
            "orders": TableSpec(layer="bronze", table="orders"),
        }
    )
    silver: TableGroup = TableGroup(
        {
            "orders": TableSpec(layer="silver", table="orders"),
        }
    )
    gold: TableGroup = TableGroup(
        {
            "orders_daily": TableSpec(layer="gold", table="orders_daily"),
        }
    )


settings = Settings()


if __name__ == "__main__":  # pragma: no cover
    import json

    print(json.dumps(settings.model_dump(), indent=2))
