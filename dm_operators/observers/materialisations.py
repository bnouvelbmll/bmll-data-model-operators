"""Refresh crd/status/*.yaml — observed status for materialisations.

This is a CRD **status subresource** refresher, not a spec generator: it does
not touch the hand-authored DataAsset/DataProduct/Resource CRDs at all. For
each materialisation in a DataAsset's `materialisations` list, it connects to
the destination backend (Snowflake or Databricks) and queries its own catalog
(INFORMATION_SCHEMA — metadata only, no warehouse scan of the actual data) to
find out:

  - Snowflake: is this table native, a Snowflake-managed Iceberg table, a
    classic external table, or a view (see resource-snowflake-marketplace.yaml's
    own note about migrating native tables to external Iceberg tables) — and
    when it IS a view, what table(s) it selects from one level deep, so
    "native" never silently means "a view over an external table".
  - Databricks: is this table MANAGED, EXTERNAL, or a VIEW, and its Delta/
    Iceberg/Parquet storage format.
  - Either backend: approximate row count and size in GB (Snowflake) or size
    in bytes (Databricks, via DESCRIBE DETAIL).
  - Whether the physical table's columns match the owning DataProduct's
    declared logical_schema (resolved via the DataAsset's `product_id` link),
    flagging columns missing from the table, extra undeclared columns, and
    (Snowflake only, reusing cicd/check_snowflake_types.py's POL-003 mapping)
    type mismatches.

and appends the size/row measurement to a rolling `recent_measurements`
history, alongside a hash of the Materialisation spec it was checked against
(so a later spec edit shows up as stale status rather than a silent
mismatch).

This is a hint for humans/tooling, not a product requirement — it is not
wired into `bmll-validate`'s policy checks. Output lives in crd/status/,
never inside the hand-authored asset/product/resource CRDs, so re-running
this script never creates diff noise there.

Usage:
    # Pilot scope (default): the 3 assets behind resource.snowflake-marketplace
    # that were the seed for this tooling.
    python3 -m dm_operators.observers.materialisations

    # Explicit scope:
    python3 -m dm_operators.observers.materialisations --assets crd/assets/equity/asset-equity-l2.yaml

    # Glob:
    python3 -m dm_operators.observers.materialisations --assets 'crd/assets/**/*.yaml'

    # Print what would be written without connecting to anything or writing files:
    python3 -m dm_operators.observers.materialisations --dry-run
"""

from __future__ import annotations

import argparse
import glob
import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

# The model is the checkout the job runs in (the current directory), or DATAMODEL_ROOT; this module is not part of it.
ROOT = Path(os.environ.get("DATAMODEL_ROOT") or Path.cwd()).resolve()
HERE = ROOT / "scripts"
STATUS_DIR = ROOT / "crd" / "status"

sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(HERE))

from cicd.check_snowflake_types import (  # noqa: E402
    _effective_sf_type,
    _load_canonical_types,
    _normalise as _normalise_sf_type,
)
from schema_range_utils import expand_column_registry  # noqa: E402

# Pilot scope: the assets behind resource.snowflake-marketplace's recently
# touched products (product.equity-l2, product.equity-ms-cbbo,
# product.equity-nbbo). Widen with --assets once this proves out.
DEFAULT_ASSETS = [
    ROOT / "crd" / "assets" / "equity" / "asset-equity-l2.yaml",
    ROOT / "crd" / "assets" / "equity" / "asset-equity-cbbo.yaml",
    ROOT / "crd" / "assets" / "equity" / "asset-equity-nbbo.yaml",
]

MAX_MEASUREMENTS = 12  # cap on recent_measurements, oldest dropped first
MAX_UNDERLYING_REFS = 5  # cap on regex-extracted view references

CHECKED_BY = "update_materialisation_status.py"

# FROM/JOIN <db>.<schema>.<table> — accepts bare, double-quoted (Snowflake),
# or backtick-quoted (Databricks) identifiers. Best-effort: only matches
# fully-qualified 3-part references, which is how Snowflake's GET_DDL and
# most generated Databricks view definitions render them.
_QUALIFIED_REF_RE = re.compile(
    r"(?:FROM|JOIN)\s+"
    r'[`"]?([\w$]+)[`"]?\.[`"]?([\w$]+)[`"]?\.[`"]?([\w$]+)[`"]?',
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------------


def _snowflake_connection():
    """Connect to Snowflake using the SNOWFLAKE_* environment variables.

    Uses externalbrowser SSO when SNOWFLAKE_USE_SSO=1 (no password env var
    is expected in that case), else falls back to SNOWFLAKE_PASSWORD.
    """
    import snowflake.connector

    connargs: dict[str, Any] = dict(
        user=os.environ.get("SNOWFLAKE_USER"),
        account=os.environ.get("SNOWFLAKE_ACCOUNT"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
        role=os.environ.get("SNOWFLAKE_ROLE"),
        database=os.environ.get("SNOWFLAKE_DATABASE"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA"),
    )
    region = os.environ.get("SNOWFLAKE_REGION")
    if region:
        connargs["region"] = region

    if os.environ.get("SNOWFLAKE_USE_SSO") == "1":
        connargs["authenticator"] = "externalbrowser"
    else:
        connargs["password"] = os.environ.get("SNOWFLAKE_PASSWORD")

    return snowflake.connector.connect(**connargs)


def _databricks_connection():
    """Connect to Databricks using the DATABRICKS_* environment variables."""
    from databricks import sql

    return sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )


# ---------------------------------------------------------------------------
# Reading DataAsset CRDs for materialisations
# ---------------------------------------------------------------------------


def _resolve_asset_paths(patterns: list[str]) -> list[Path]:
    paths: list[Path] = []
    for pattern in patterns:
        matches = sorted(glob.glob(pattern, recursive=True))
        if matches:
            paths.extend(Path(m) for m in matches)
        else:
            paths.append(Path(pattern))
    return paths


def _lineage_product_id(doc: dict, _seen: set | None = None) -> str | None:
    """The product a row-limited customer view (sample, trial window) belongs to.

    Such a view declares no ``product_id``: it is the same dataset cut down by rows, linked to its parent asset only through
    ``lineage.inputs``. Follow that link upwards so the view is checked against the same logical schema as the parent.
    """
    if doc.get("product_id"):
        return doc["product_id"]
    props = ((doc.get("lineage") or {}).get("transform") or {}).get("properties") or []
    inputs = [i for i in ((doc.get("lineage") or {}).get("inputs") or []) if isinstance(i, dict)]
    only_filters = bool(props) and all(p == "filter" for p in props)
    if not inputs or not (only_filters or any(i.get("filter") for i in inputs)):
        return None
    seen = _seen or set()
    for parent_id in (i.get("asset_id") for i in inputs):
        if not parent_id or parent_id in seen:
            continue
        seen.add(parent_id)
        parent = _asset_index().get(parent_id)
        found = _lineage_product_id(parent, seen) if parent else None
        if found:
            return found
    return None


_ASSET_INDEX: dict[str, dict] | None = None


def _asset_index() -> dict[str, dict]:
    """Every DataAsset CRD by id (loaded once)."""
    global _ASSET_INDEX
    if _ASSET_INDEX is None:
        _ASSET_INDEX = {}
        for path in glob.glob(str(ROOT / "crd/assets/**/*.yaml"), recursive=True):
            try:
                doc = yaml.safe_load(Path(path).read_text()) or {}
            except yaml.YAMLError:
                continue
            if isinstance(doc, dict) and doc.get("id"):
                _ASSET_INDEX[doc["id"]] = doc
    return _ASSET_INDEX


def _iter_materialisations(asset_paths: list[Path]):
    """Yield one dict per materialisation with a backend we can check.

    Keys: subject_id, product_id, backend, materialisation_name,
    destination_location, spec (the raw Materialisation dict),
    partition_strategy (the raw DataAsset partition_strategy dict, if any).
    """
    for path in asset_paths:
        if not path.exists():
            print(f"  SKIP {path}: file not found")
            continue
        doc = yaml.safe_load(path.read_text()) or {}
        subject_id = doc.get("id")
        if not subject_id:
            continue
        for spec in doc.get("materialisations") or []:
            destination = (spec or {}).get("destination") or {}
            backend = destination.get("backend")
            if backend not in ("snowflake", "databricks"):
                continue
            yield {
                "subject_id": subject_id,
                "product_id": _lineage_product_id(doc),
                "backend": backend,
                "materialisation_name": spec.get("name"),
                "destination_location": destination.get("location"),
                "spec": spec,
                "partition_strategy": doc.get("partition_strategy"),
            }


def _spec_hash(spec: dict) -> str:
    """Stable hash of a Materialisation spec dict, for drift detection."""
    canonical = json.dumps(spec, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Declared schema — column registry + owning DataProduct's logical_schema
# ---------------------------------------------------------------------------


def _load_column_registry() -> dict[str, dict]:
    """id -> expanded Column record, from crd/columns/columns-*.yaml."""
    raw: dict[str, dict] = {}
    for path in sorted((ROOT / "crd" / "columns").glob("columns-*.yaml")):
        data = yaml.safe_load(path.read_text()) or {}
        for col in data.get("columns") or []:
            if isinstance(col, dict) and col.get("id"):
                raw[col["id"]] = col
    return expand_column_registry(raw)


def _load_product_column_ids(product_id: str) -> list[str] | None:
    """Fully-expanded col ids from a DataProduct's logical_schema, by id."""
    for path in (ROOT / "crd" / "products").glob("*.yaml"):
        doc = yaml.safe_load(path.read_text()) or {}
        if doc.get("id") != product_id:
            continue
        columns = (doc.get("logical_schema") or {}).get("columns") or []
        return [c["col"] for c in columns if isinstance(c, dict) and c.get("col")]
    return None


def _declared_snowflake_schema(
    col_ids: list[str], registry: dict[str, dict], canonical_types: dict[str, str | None]
) -> dict[str, str | None]:
    """physical_name (upper) -> effective canonical Snowflake type (or None)."""
    declared: dict[str, str | None] = {}
    for col_id in col_ids:
        record = registry.get(col_id)
        sf = (record.get("materialisations") or {}).get("snowflake") if record else None
        phys_name = (sf or {}).get("nat_name") or col_id.rsplit(".", 1)[-1].upper()
        eff_type = _effective_sf_type(record, canonical_types) if record else None
        declared[phys_name.upper()] = eff_type
    return declared


def _declared_databricks_columns(col_ids: list[str], registry: dict[str, dict]) -> set[str]:
    """Set of expected lowercase physical column names for Databricks."""
    names: set[str] = set()
    for col_id in col_ids:
        record = registry.get(col_id)
        db = (record.get("materialisations") or {}).get("databricks") if record else None
        phys_name = (db or {}).get("pq_name") or col_id.rsplit(".", 1)[-1]
        names.add(phys_name.lower())
    return names


# ---------------------------------------------------------------------------
# Snowflake: classify, measure, resolve views, check schema
# ---------------------------------------------------------------------------


def _parse_location3(location: str) -> tuple[str, str, str]:
    parts = location.split(".")
    if len(parts) != 3:
        raise ValueError(f"Expected a 3-part db.schema.table location, got: {location!r}")
    return tuple(p.upper() for p in parts)  # type: ignore[return-value]


def _classify_snowflake(table_type: str, is_iceberg: str | None) -> str:
    if table_type == "EXTERNAL TABLE":
        return "external"
    if table_type == "VIEW":
        return "view"
    if table_type == "DYNAMIC TABLE":
        return "dynamic_table"
    if table_type == "BASE TABLE":
        return "iceberg" if (is_iceberg or "").upper() == "YES" else "native"
    return "unknown"


def _query_snowflake_meta(cur, location: str) -> dict:
    """Metadata-only lookup in INFORMATION_SCHEMA.TABLES — never scans data."""
    db, schema, table = _parse_location3(location)
    cur.execute(
        f'SELECT TABLE_TYPE, IS_ICEBERG, ROW_COUNT, BYTES, CLUSTERING_KEY, '
        f'RETENTION_TIME, CREATED '
        f'FROM "{db}".INFORMATION_SCHEMA.TABLES '
        f"WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
        (schema, table),
    )
    row = cur.fetchone()
    if row is None:
        raise LookupError(f"{location}: not found in {db}.INFORMATION_SCHEMA.TABLES")
    table_type, is_iceberg, row_count, num_bytes, clustering_key, retention_time, created = row
    return {
        "table_kind": _classify_snowflake(table_type, is_iceberg),
        "clustering_key": clustering_key,
        "retention_time_days": int(retention_time) if retention_time is not None else None,
        "created_on": created.isoformat() if created is not None else None,
        "row_count": int(row_count) if row_count is not None else None,
        "bytes": int(num_bytes) if num_bytes is not None else None,
    }


def _resolve_snowflake_view(cur, location: str) -> list[dict]:
    """Best-effort: table(s) a Snowflake view selects from, one level deep."""
    db, schema, table = _parse_location3(location)
    try:
        cur.execute("SELECT GET_DDL('VIEW', %s)", (f"{db}.{schema}.{table}",))
        (ddl,) = cur.fetchone()
    except Exception as exc:
        print(f"    WARN: could not fetch view DDL for {location}: {exc}")
        return []

    underlying: list[dict] = []
    seen: set[str] = set()
    for ref_db, ref_schema, ref_table in _QUALIFIED_REF_RE.findall(ddl or ""):
        loc = f"{ref_db}.{ref_schema}.{ref_table}".lower()
        if loc in seen or len(underlying) >= MAX_UNDERLYING_REFS:
            continue
        seen.add(loc)
        try:
            meta = _query_snowflake_meta(cur, loc)
            kind = meta["table_kind"]
        except Exception:
            kind = "unknown"
        underlying.append({"location": loc, "table_kind": kind})
    return underlying


def _query_partition_count_snowflake(cur, location: str, partition_columns: list[str]) -> int | None:
    """Best-effort COUNT(DISTINCT <col>) for the first declared partition column.

    Only run when the owning DataAsset declares a partition_strategy — this
    is the one query in this script that scans real data, so it stays opt-in.
    """
    if not partition_columns:
        return None
    db, schema, table = _parse_location3(location)
    col = partition_columns[0].rsplit(".", 1)[-1].upper()
    cur.execute(f'SELECT COUNT(DISTINCT "{col}") FROM "{db}"."{schema}"."{table}"')
    (count,) = cur.fetchone()
    return int(count) if count is not None else None


def _reconstruct_sf_type(data_type: str, char_len, num_prec, num_scale, datetime_prec) -> str:
    dt = (data_type or "").upper()
    if dt == "TEXT":
        return f"VARCHAR({int(char_len)})" if char_len else "VARCHAR(16777216)"
    if dt == "NUMBER":
        if num_prec is not None and num_scale is not None:
            return f"NUMBER({int(num_prec)},{int(num_scale)})"
        return "NUMBER"
    if dt in ("TIME", "TIMESTAMP_NTZ", "TIMESTAMP_TZ", "TIMESTAMP_LTZ") and datetime_prec is not None:
        return f"{dt}({int(datetime_prec)})"
    return dt  # FLOAT, BOOLEAN, DATE, ... already comparable


def _actual_snowflake_columns(cur, location: str) -> dict[str, str]:
    db, schema, table = _parse_location3(location)
    cur.execute(
        f'SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, '
        f'NUMERIC_SCALE, DATETIME_PRECISION '
        f'FROM "{db}".INFORMATION_SCHEMA.COLUMNS '
        f"WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
        (schema, table),
    )
    actual: dict[str, str] = {}
    for name, data_type, char_len, num_prec, num_scale, datetime_prec in cur.fetchall():
        actual[name.upper()] = _reconstruct_sf_type(data_type, char_len, num_prec, num_scale, datetime_prec)
    return actual


def _check_snowflake_schema(
    cur, location: str, product_id: str, col_ids: list[str], registry: dict, canonical_types: dict
) -> dict:
    declared = _declared_snowflake_schema(col_ids, registry, canonical_types)
    actual = _actual_snowflake_columns(cur, location)
    return _compare_schema(product_id, declared, actual, compare_types=True)


def _compare_schema(
    product_id: str,
    declared: dict[str, str | None],
    actual: dict[str, str],
    compare_types: bool,
) -> dict:
    declared_names = set(declared)
    actual_names = set(actual)
    missing = sorted(declared_names - actual_names)
    extra = sorted(actual_names - declared_names)
    mismatches: list[str] = []
    if compare_types:
        for name in sorted(declared_names & actual_names):
            d_type = _normalise_sf_type(declared[name]) if declared.get(name) else None
            a_type = _normalise_sf_type(actual[name])
            if d_type and a_type and d_type != a_type:
                mismatches.append(f"{name}: declared {d_type!r} != actual {a_type!r}")
    return {
        "checked_against": product_id,
        "missing_columns": missing or None,
        "extra_columns": extra or None,
        "type_mismatches": mismatches or None,
        "ok": not missing and not extra and not mismatches,
    }


# ---------------------------------------------------------------------------
# Databricks: classify, measure, resolve views, check schema
# ---------------------------------------------------------------------------


def _parse_location_flexible(location: str) -> tuple[str | None, str, str]:
    """Databricks locations may be catalog.schema.table or just schema.table."""
    parts = location.split(".")
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return None, parts[0], parts[1]
    raise ValueError(f"Expected schema.table or catalog.schema.table, got: {location!r}")


def _databricks_info_schema_prefix(catalog: str | None) -> str:
    return f"{catalog}.information_schema" if catalog else "information_schema"


def _query_databricks_meta(cur, location: str) -> dict:
    catalog, schema, table = _parse_location_flexible(location)
    prefix = _databricks_info_schema_prefix(catalog)
    cur.execute(
        f"SELECT table_type, data_source_format FROM {prefix}.tables "
        f"WHERE table_schema = %s AND table_name = %s",
        (schema, table),
    )
    row = cur.fetchone()
    if row is None:
        raise LookupError(f"{location}: not found in {prefix}.tables")
    table_type, data_source_format = row
    return {
        "table_kind": (table_type or "unknown").lower(),
        "data_source_format": data_source_format,
    }


def _query_databricks_size(cur, location: str) -> dict:
    """DESCRIBE DETAIL — cheap Delta/Iceberg table metadata, no data scan.

    Fails for views and some non-Delta formats; caller treats that as
    "size unavailable" rather than an error.
    """
    try:
        cur.execute(f"DESCRIBE DETAIL {location}")
        columns = [d[0] for d in cur.description]
        row = cur.fetchone()
    except Exception:
        return {"size_gb": None, "row_count": None}
    if row is None:
        return {"size_gb": None, "row_count": None}
    detail = dict(zip(columns, row))
    size_bytes = detail.get("sizeInBytes")
    return {
        "size_gb": round(size_bytes / (1024**3), 4) if size_bytes is not None else None,
        "row_count": None,  # not cheaply available without a data scan
    }


def _resolve_databricks_view(cur, location: str) -> list[dict]:
    """Best-effort: table(s) a Databricks view selects from, one level deep."""
    catalog, schema, table = _parse_location_flexible(location)
    prefix = _databricks_info_schema_prefix(catalog)
    try:
        cur.execute(
            f"SELECT view_definition FROM {prefix}.views "
            f"WHERE table_schema = %s AND table_name = %s",
            (schema, table),
        )
        row = cur.fetchone()
    except Exception as exc:
        print(f"    WARN: could not fetch view definition for {location}: {exc}")
        return []
    if row is None:
        return []
    (ddl,) = row

    underlying: list[dict] = []
    seen: set[str] = set()
    for ref_db, ref_schema, ref_table in _QUALIFIED_REF_RE.findall(ddl or ""):
        loc = f"{ref_db}.{ref_schema}.{ref_table}".lower()
        if loc in seen or len(underlying) >= MAX_UNDERLYING_REFS:
            continue
        seen.add(loc)
        try:
            meta = _query_databricks_meta(cur, loc)
            kind = meta["table_kind"]
        except Exception:
            kind = "unknown"
        underlying.append({"location": loc, "table_kind": kind})
    return underlying


def _actual_databricks_columns(cur, location: str) -> set[str]:
    catalog, schema, table = _parse_location_flexible(location)
    prefix = _databricks_info_schema_prefix(catalog)
    cur.execute(
        f"SELECT column_name FROM {prefix}.columns "
        f"WHERE table_schema = %s AND table_name = %s",
        (schema, table),
    )
    return {row[0].lower() for row in cur.fetchall()}


def _check_databricks_schema(
    cur, location: str, product_id: str, col_ids: list[str], registry: dict
) -> dict:
    declared_names = _declared_databricks_columns(col_ids, registry)
    actual_names = _actual_databricks_columns(cur, location)
    return _compare_schema(
        product_id,
        {name: None for name in declared_names},
        {name: "" for name in actual_names},
        compare_types=False,
    )


# ---------------------------------------------------------------------------
# Writing crd/status/*.yaml
# ---------------------------------------------------------------------------


def _status_path(subject_id: str, materialisation_name: str | None) -> Path:
    name_part = materialisation_name or "default"
    return STATUS_DIR / f"{subject_id}.{name_part}.yaml"


def _load_existing(path: Path) -> dict:
    if path.exists():
        return yaml.safe_load(path.read_text()) or {}
    return {}


def _build_status_doc(
    m: dict,
    backend_status: dict | None,
    measurement: dict | None,
    schema_check: dict | None,
    error: str | None,
    existing: dict,
) -> dict:
    measurements = list(existing.get("recent_measurements") or [])
    if measurement is not None:
        measurements.append(measurement)
        measurements = measurements[-MAX_MEASUREMENTS:]

    doc = {
        "id": f"status.{m['subject_id']}.{m['materialisation_name'] or 'default'}",
        "subject": m["subject_id"],
        "materialisation_name": m["materialisation_name"],
        "destination_location": m["destination_location"],
        "schemaschemer_path": existing.get("schemaschemer_path"),
        "snowflake": backend_status if m["backend"] == "snowflake" else existing.get("snowflake"),
        "databricks": backend_status if m["backend"] == "databricks" else existing.get("databricks"),
        "schema_check": schema_check,
        "recent_measurements": measurements or None,
        "last_applied_spec_hash": _spec_hash(m["spec"]),
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "checked_by": CHECKED_BY,
        "error": error,
    }
    return {k: v for k, v in doc.items() if v is not None}


def _process_snowflake(cur, m: dict, registry: dict, canonical_types: dict) -> tuple[dict, dict, dict | None]:
    location = m["destination_location"]
    meta = _query_snowflake_meta(cur, location)
    status = {
        "table_kind": meta["table_kind"],
        "clustering_key": meta["clustering_key"],
        "retention_time_days": meta["retention_time_days"],
        "created_on": meta["created_on"],
    }
    if meta["table_kind"] == "view":
        status["underlying_tables"] = _resolve_snowflake_view(cur, location) or None

    measurement = {
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "size_gb": round(meta["bytes"] / (1024**3), 4) if meta["bytes"] is not None else None,
        "row_count": meta["row_count"],
        "partition_count": None,
    }
    partition_strategy = m.get("partition_strategy") or {}
    if partition_strategy.get("columns"):
        try:
            measurement["partition_count"] = _query_partition_count_snowflake(
                cur, location, partition_strategy["columns"]
            )
        except Exception as exc:  # best-effort only
            print(f"    WARN: partition count failed: {exc}")

    schema_check = None
    if m.get("product_id"):
        col_ids = _load_product_column_ids(m["product_id"])
        if col_ids:
            schema_check = _check_snowflake_schema(
                cur, location, m["product_id"], col_ids, registry, canonical_types
            )
        else:
            print(f"    WARN: could not resolve logical_schema for {m['product_id']}")

    return status, measurement, schema_check


def _process_databricks(cur, m: dict, registry: dict) -> tuple[dict, dict, dict | None]:
    location = m["destination_location"]
    meta = _query_databricks_meta(cur, location)
    status = {
        "table_kind": meta["table_kind"],
        "data_source_format": meta["data_source_format"],
    }
    if meta["table_kind"] == "view":
        status["underlying_tables"] = _resolve_databricks_view(cur, location) or None

    size = _query_databricks_size(cur, location)
    measurement = {
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "size_gb": size["size_gb"],
        "row_count": size["row_count"],
        "partition_count": None,
    }

    schema_check = None
    if m.get("product_id"):
        col_ids = _load_product_column_ids(m["product_id"])
        if col_ids:
            schema_check = _check_databricks_schema(cur, location, m["product_id"], col_ids, registry)
        else:
            print(f"    WARN: could not resolve logical_schema for {m['product_id']}")

    return status, measurement, schema_check


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--assets",
        nargs="+",
        default=None,
        help="DataAsset YAML paths or globs to check (default: the pilot scope)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be checked without connecting to anything or writing files",
    )
    args = parser.parse_args()

    asset_paths = _resolve_asset_paths(args.assets) if args.assets else DEFAULT_ASSETS

    materialisations = list(_iter_materialisations(asset_paths))
    if not materialisations:
        print("No snowflake/databricks materialisations found in the given assets.")
        return

    print(f"Found {len(materialisations)} materialisation(s):")
    for m in materialisations:
        print(f"  {m['subject_id']} ({m['backend']}/{m['materialisation_name']}) -> {m['destination_location']}")

    if args.dry_run:
        print("\n--dry-run: not connecting to Snowflake/Databricks or writing files.")
        return

    registry = _load_column_registry()
    canonical_types = _load_canonical_types()

    STATUS_DIR.mkdir(parents=True, exist_ok=True)

    sf_con = None
    db_con = None
    try:
        for m in materialisations:
            path = _status_path(m["subject_id"], m["materialisation_name"])
            existing = _load_existing(path)
            status = None
            measurement = None
            schema_check = None
            error = None
            try:
                if m["backend"] == "snowflake":
                    if sf_con is None:
                        sf_con = _snowflake_connection()
                    with sf_con.cursor() as cur:
                        status, measurement, schema_check = _process_snowflake(
                            cur, m, registry, canonical_types
                        )
                else:
                    if db_con is None:
                        db_con = _databricks_connection()
                    with db_con.cursor() as cur:
                        status, measurement, schema_check = _process_databricks(cur, m, registry)
            except Exception as exc:
                error = str(exc)
                print(f"  ERROR {m['subject_id']} ({m['destination_location']}): {exc}")

            doc = _build_status_doc(m, status, measurement, schema_check, error, existing)
            header = (
                f"# GENERATED by scripts/{CHECKED_BY} — do not hand-edit.\n"
                f"# Observed {m['backend']} status for {m['subject_id']}'s "
                f"{m['materialisation_name']!r} materialisation.\n"
            )
            path.write_text(header + yaml.safe_dump(doc, sort_keys=False, default_flow_style=False))
            print(f"  Wrote {path}")
    finally:
        if sf_con is not None:
            sf_con.close()
        if db_con is not None:
            db_con.close()


if __name__ == "__main__":
    main()
