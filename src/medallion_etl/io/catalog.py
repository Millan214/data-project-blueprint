from medallion_etl.settings import settings

LAYER_DIRS = {"bronze": "01_bronze", "silver": "02_silver", "gold": "03_gold"}


def uri_for(layer: str, table: str, sub: str = "") -> str:
    base = f"{settings.storage_root}/{LAYER_DIRS[layer]}"
    parts = [base, sub, table] if sub else [base, table]
    return "/".join(p.strip("/") for p in parts if p)
