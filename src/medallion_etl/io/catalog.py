from medallion_etl.settings import settings


def _layer_dir(layer: str) -> str:
    mapping = {
        "bronze": settings.bronze_dir,
        "silver": settings.silver_dir,
        "gold": settings.gold_dir,
    }
    try:
        return mapping[layer]
    except KeyError as exc:
        raise ValueError(f"unknown layer: {layer!r}") from exc


def uri_for(layer: str, table: str, sub: str = "") -> str:
    base = f"{settings.storage_root}/{_layer_dir(layer)}"
    parts = [base, sub, table] if sub else [base, table]
    return "/".join(p.strip("/") for p in parts if p)
