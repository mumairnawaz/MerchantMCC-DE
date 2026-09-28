"""Client Data Delivery Layer.

Reads existing dbt marts (data/gold/gold.duckdb, schema `marts`) — never
Bronze, Silver, Gold facts/dimensions, staging, or intermediate models
directly (Client Data Delivery Audit §3: marts are already the purpose-
built, consumption-shaped layer this project's own docs/27 §21 describes,
so this module adds no new transformation/aggregation logic of its own).

DuckDB only (no new database) for reading; PyArrow only for CSV/Parquet
generation — both are already core project dependencies (pyproject.toml),
no new dependency introduced.

Every write to outbox/ is all-or-nothing: the extract, its read-back
verification, and its manifest are all built in a temporary staging
directory first; the finished, fully-validated directory is moved into
outbox/ only once every gate in `_validate_extract` has passed. Any
exception raised before that point leaves outbox/ completely untouched —
never a partially-written or falsely-successful delivery.
"""

import json
import shutil
import tempfile
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pa_csv
import pyarrow.parquet as pa_parquet

from src.delivery.config import assert_authorized, get_dataset_definition
from src.delivery.errors import DeliveryValidationError
from src.ingestion.common import utc_now_iso

GOLD_DUCKDB_PATH = Path("data/gold/gold.duckdb")
OUTBOX_ROOT = Path("outbox")


# ---- small JSON helpers (Decimal/date are not natively JSON-serializable) ----


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"not JSON serializable: {value!r}")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, default=_json_default), encoding="utf-8")


# ---- SQL construction (all identifiers come from the trusted registry, never
# from caller input; only the client_id VALUE is parameterized) ----


def _build_where(dataset_def: dict[str, Any], client_id: str | None) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if dataset_def.get("requires_client_filter"):
        clauses.append("client_id = ?")
        params.append(client_id)
    if dataset_def.get("control_total_filter"):
        # applied only to the control-total SUM query, never to the extract
        # itself — see _compute_control_total()
        pass
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return where_sql, params


def _fetch_extract(con: duckdb.DuckDBPyConnection, dataset_name: str, dataset_def: dict[str, Any], client_id: str | None) -> pa.Table:
    cols_sql = ", ".join(dataset_def["expected_columns"])
    grain_sql = ", ".join(dataset_def["business_grain"])
    where_sql, params = _build_where(dataset_def, client_id)
    query = f"SELECT {cols_sql} FROM marts.{dataset_name} {where_sql} ORDER BY {grain_sql}"
    return con.execute(query, params).to_arrow_table()


def _compute_source_control_total(
    con: duckdb.DuckDBPyConnection, dataset_name: str, dataset_def: dict[str, Any], client_id: str | None
) -> Decimal | None:
    control_col = dataset_def.get("control_total_column")
    if control_col is None:
        return None
    where_sql, params = _build_where(dataset_def, client_id)
    extra_filter = dataset_def.get("control_total_filter")
    if extra_filter:
        where_sql = f"{where_sql} AND {extra_filter}" if where_sql else f"WHERE {extra_filter}"
    query = f"SELECT SUM({control_col}) FROM marts.{dataset_name} {where_sql}"
    result = con.execute(query, params).fetchone()[0]
    return Decimal(str(result)) if result is not None else Decimal("0")


def _compute_extract_control_total(table: pa.Table, dataset_def: dict[str, Any]) -> Decimal | None:
    control_col = dataset_def.get("control_total_column")
    if control_col is None:
        return None
    extra_filter = dataset_def.get("control_total_filter")
    values = table.column(control_col)
    if extra_filter:
        # Only the one real case in this registry: "transaction_status = 'APPROVED'".
        # Parsed defensively rather than eval'd — this project never executes
        # dynamic Python from config.
        field, _, literal = extra_filter.partition("=")
        field = field.strip()
        literal = literal.strip().strip("'")
        mask = pc.equal(table.column(field), pa.scalar(literal))
        values = pc.filter(values, mask)
    total = pc.sum(values).as_py()
    return Decimal(str(total)) if total is not None else Decimal("0")


def _isoformat_or_none(value: Any) -> str | None:
    return value.isoformat() if value is not None else None


def _compute_reporting_period(table: pa.Table, dataset_def: dict[str, Any]) -> dict[str, Any]:
    """Returns plain, already-JSON-safe values (ISO date strings, not `date`
    objects) — this dict is compared directly against manifests re-loaded from
    disk (idempotency check, _find_existing_valid_delivery) and returned
    as-is in the result dict, so it must be identical in both places, not
    just after a json.dumps/loads round-trip."""
    date_col = dataset_def.get("date_column")
    start_col = dataset_def.get("period_start_column")
    end_col = dataset_def.get("period_end_column")
    if date_col:
        col = table.column(date_col)
        if table.num_rows == 0:
            return {"start": None, "end": None}
        return {"start": _isoformat_or_none(pc.min(col).as_py()), "end": _isoformat_or_none(pc.max(col).as_py())}
    if start_col and end_col:
        if table.num_rows == 0:
            return {"start": None, "end": None}
        return {
            "start": _isoformat_or_none(pc.min(table.column(start_col)).as_py()),
            "end": _isoformat_or_none(pc.max(table.column(end_col)).as_py()),
        }
    return {"semantics": "full_snapshot_no_date_dimension_in_mart"}


# ---- validation gates (Client Data Delivery Audit §10) ----


def _validate_extract(
    *,
    dataset_name: str,
    client_id: str,
    dataset_def: dict[str, Any],
    table: pa.Table,
    source_control_total: Decimal | None,
    extract_control_total: Decimal | None,
) -> None:
    expected_cols = dataset_def["expected_columns"]
    if table.column_names != expected_cols:
        raise DeliveryValidationError(
            f"{dataset_name}: extract columns {table.column_names} do not match "
            f"schema_version {dataset_def['schema_version']}'s expected {expected_cols}"
        )

    grain_cols = dataset_def["business_grain"]
    grain_tuples = list(zip(*[table.column(c).to_pylist() for c in grain_cols]))
    if len(set(grain_tuples)) != table.num_rows:
        raise DeliveryValidationError(
            f"{dataset_name}: {table.num_rows} rows but only {len(set(grain_tuples))} "
            f"distinct {grain_cols} values — unexpected duplicate business grain"
        )

    for col in grain_cols:
        if table.column(col).null_count > 0:
            raise DeliveryValidationError(f"{dataset_name}: grain column '{col}' contains NULLs")

    if source_control_total is not None and extract_control_total is not None:
        if source_control_total != extract_control_total:
            raise DeliveryValidationError(
                f"{dataset_name} ({client_id}): control total mismatch — "
                f"source mart={source_control_total}, extract={extract_control_total}"
            )


def _validate_readback(path: Path, fmt: str, expected_row_count: int) -> int:
    if fmt == "csv":
        read_back = pa_csv.read_csv(path)
    else:
        read_back = pa_parquet.read_table(path)
    if read_back.num_rows != expected_row_count:
        raise DeliveryValidationError(
            f"{path.name}: read-back row count {read_back.num_rows} != expected {expected_row_count}"
        )
    return read_back.num_rows


# ---- idempotency (Client Data Delivery Audit §11: client + dataset + period + schema_version) ----


def _find_existing_valid_delivery(
    outbox_root: Path, client_id: str, dataset_name: str, reporting_period: dict[str, Any], schema_version: str
) -> dict[str, Any] | None:
    dataset_dir = outbox_root / client_id / dataset_name
    if not dataset_dir.exists():
        return None
    for run_dir in sorted(dataset_dir.iterdir()):
        manifest_path = run_dir / "manifest.json"
        if not manifest_path.exists():
            continue
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if (
            manifest.get("validation_status") == "PASSED"
            and manifest.get("schema_version") == schema_version
            and manifest.get("reporting_period") == reporting_period
        ):
            return manifest
    return None


# ---- entry point ----


def run(
    *,
    client_id: str,
    dataset_name: str,
    formats: list[str] | None = None,
    gold_duckdb_path: Path = GOLD_DUCKDB_PATH,
    outbox_root: Path = OUTBOX_ROOT,
    force: bool = False,
) -> dict[str, Any]:
    """Generate one client's delivery of one dataset. Raises UnknownClientError /
    UnknownDatasetError / UnauthorizedDatasetError / DeliveryValidationError —
    never returns a result dict for a delivery that didn't actually succeed."""
    dataset_def = get_dataset_definition(dataset_name)
    assert_authorized(client_id, dataset_name)

    requested_formats = formats or dataset_def["output_formats"]
    unsupported = set(requested_formats) - set(dataset_def["output_formats"])
    if unsupported:
        raise DeliveryValidationError(f"{dataset_name} does not support format(s) {unsupported}")

    client_filter = client_id if dataset_def.get("requires_client_filter") else None

    con = duckdb.connect(str(gold_duckdb_path), read_only=True)
    try:
        table = _fetch_extract(con, dataset_name, dataset_def, client_filter)
        source_total = _compute_source_control_total(con, dataset_name, dataset_def, client_filter)
    finally:
        con.close()

    extract_total = _compute_extract_control_total(table, dataset_def)
    reporting_period = _compute_reporting_period(table, dataset_def)
    schema_version = dataset_def["schema_version"]

    _validate_extract(
        dataset_name=dataset_name,
        client_id=client_id,
        dataset_def=dataset_def,
        table=table,
        source_control_total=source_total,
        extract_control_total=extract_total,
    )

    existing = _find_existing_valid_delivery(outbox_root, client_id, dataset_name, reporting_period, schema_version)
    if existing is not None and not force:
        return {
            "status": "skipped_existing",
            "client_id": client_id,
            "dataset": dataset_name,
            "existing_run_id": existing["run_id"],
            "reporting_period": reporting_period,
        }

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        written_formats: list[str] = []
        for fmt in requested_formats:
            out_path = tmp_dir / f"{dataset_name}.{fmt}"
            if fmt == "csv":
                pa_csv.write_csv(table, out_path)
            elif fmt == "parquet":
                pa_parquet.write_table(table, out_path)
            else:
                raise DeliveryValidationError(f"unsupported format '{fmt}'")
            _validate_readback(out_path, fmt, table.num_rows)
            written_formats.append(fmt)

        if len(written_formats) > 1:
            # CSV/Parquet logical consistency: same row count from both read-backs
            # (already individually confirmed above) — cross-check them directly.
            counts = {fmt: pa_csv.read_csv(tmp_dir / f"{dataset_name}.{fmt}").num_rows if fmt == "csv"
                      else pa_parquet.read_table(tmp_dir / f"{dataset_name}.{fmt}").num_rows
                      for fmt in written_formats}
            if len(set(counts.values())) != 1:
                raise DeliveryValidationError(f"{dataset_name}: CSV/Parquet row counts disagree: {counts}")

        run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
        manifest = {
            "run_id": run_id,
            "client_id": client_id,
            "dataset": dataset_name,
            "schema_version": schema_version,
            "generated_at_utc": utc_now_iso(),
            "reporting_period": reporting_period,
            "output_formats": written_formats,
            "record_count": table.num_rows,
            "control_total": str(extract_total) if extract_total is not None else "NOT_APPLICABLE",
            "control_total_column": dataset_def.get("control_total_column") or "NOT_APPLICABLE",
            "dq_status": "PASS",
            "validation_status": "PASSED",
        }
        _write_json(tmp_dir / "manifest.json", manifest)

        final_dir = outbox_root / client_id / dataset_name / run_id
        final_dir.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(tmp_dir), str(final_dir))

    return {
        "status": "delivered",
        "client_id": client_id,
        "dataset": dataset_name,
        "run_id": run_id,
        "output_dir": str(final_dir),
        "record_count": table.num_rows,
        "manifest": manifest,
    }


def validate_marts_available(gold_duckdb_path: Path = GOLD_DUCKDB_PATH, dataset_names: list[str] | None = None) -> dict[str, Any]:
    """Cheap pre-check (the Airflow `validate_marts` task): confirms each
    registered mart exists and exposes its declared expected_columns, without
    generating any extract. Raises DeliveryValidationError naming every
    problem found, not just the first."""
    from src.delivery.config import load_dataset_registry

    registry = load_dataset_registry()
    names = dataset_names or list(registry.keys())
    problems: list[str] = []
    con = duckdb.connect(str(gold_duckdb_path), read_only=True)
    try:
        for name in names:
            dataset_def = registry[name]
            try:
                actual_cols = {
                    row[0]
                    for row in con.execute(
                        "SELECT column_name FROM information_schema.columns WHERE table_schema='marts' AND table_name = ?",
                        [name],
                    ).fetchall()
                }
            except Exception as exc:  # noqa: BLE001 - surfaced as a named problem, not swallowed
                problems.append(f"{name}: could not query marts.{name} ({exc})")
                continue
            missing = set(dataset_def["expected_columns"]) - actual_cols
            if not actual_cols:
                problems.append(f"{name}: marts.{name} does not exist or has no columns")
            elif missing:
                problems.append(f"{name}: marts.{name} is missing expected columns {missing}")
    finally:
        con.close()

    if problems:
        raise DeliveryValidationError("validate_marts_available: " + "; ".join(problems))
    return {"status": "ok", "checked": names}
