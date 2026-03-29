"""
DAG: ingest_odoo_to_dw
Description: Incremental ELT pipeline — Odoo Commercial data → PostgreSQL Data Warehouse.
             Implements watermark-based incremental extraction with chunked loading,
             checkpoint/resume for large tables, and blocking data quality gates.

Author:  Data Engineering
Version: 3.0 — Portfolio Edition
"""

from __future__ import annotations

import json
import logging
import math
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from io import StringIO

import numpy as np
import pandas as pd
import psycopg2
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone
from airflow.utils.task_group import TaskGroup

# ─────────────────────────────────────────────────────────────
# DAG DEFAULT ARGUMENTS
# ─────────────────────────────────────────────────────────────

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": True,
    "email": ["data-team@company.com"],  # configure via Airflow Variables
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

dag = DAG(
    "ingest_odoo_to_dw",
    default_args=default_args,
    description="Incremental commercial ELT: Odoo → DW (watermark + chunked)",
    schedule_interval="0 2 * * *",
    catchup=False,
    tags=["etl", "odoo", "commercial", "production"],
    doc_md=__doc__,
)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION  (no credentials — all injected via env / Airflow connections)
# ─────────────────────────────────────────────────────────────

ODOO_CONN_ID = "odoo_postgres"   # defined in Airflow Connections UI
DW_CONN_ID   = "dw_postgres"     # defined in Airflow Connections UI

# Tables processed in parallel (standard pool)
STANDARD_TABLES = [
    "crm_lead",
    "sale_order",
    "res_partner",
    "res_users",
    "crm_team",
    "crm_stage",
    "crm_lost_reason",
    "res_company",
    "hr_employee",
    "ir_model_fields",
]

# Tables processed with chunking (heavy_tables_pool — limited concurrency)
HEAVY_TABLES = [
    "mail_message",
    "mail_tracking_value",
]

# Per-table tracking field (Odoo audit columns)
TABLE_CONFIG: dict[str, dict] = {
    "crm_lead":            {"date_field": "write_date"},
    "sale_order":          {"date_field": "write_date"},
    "res_partner":         {"date_field": "write_date"},
    "res_users":           {"date_field": "write_date"},
    "crm_team":            {"date_field": "write_date"},
    "crm_stage":           {"date_field": "write_date"},
    "crm_lost_reason":     {"date_field": "write_date"},
    "res_company":         {"date_field": "write_date"},
    "hr_employee":         {"date_field": "write_date"},
    "ir_model_fields":     {"date_field": "write_date"},
    "mail_tracking_value": {"date_field": "create_date"},
    "mail_message":        {"date_field": "date"},
}

CHUNK_SIZE      = 10_000   # rows per incremental chunk
HEAVY_CHUNKS    = 5        # number of ID-range chunks for heavy tables
DB_TIMEOUT_MS   = 600_000  # 10 min statement timeout on Odoo connection
MAX_RETRIES     = 3
RETRY_BASE_SECS = 5

# ─────────────────────────────────────────────────────────────
# EXCEPTIONS
# ─────────────────────────────────────────────────────────────


class DataQualityError(AirflowException):
    """Raised when a blocking quality gate fails — triggers rollback."""


# ─────────────────────────────────────────────────────────────
# CONNECTION HELPERS
# ─────────────────────────────────────────────────────────────


@contextmanager
def odoo_connection():
    """
    Yields a read-only psycopg2 connection to Odoo PostgreSQL.
    Credentials come exclusively from the Airflow 'odoo_postgres' Connection object.
    Implements exponential-backoff retry for transient network errors.
    """
    hook = PostgresHook(postgres_conn_id=ODOO_CONN_ID)
    conn = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            conn = hook.get_conn()
            # Enforce read-only + statement timeout on every session
            with conn.cursor() as cur:
                cur.execute(f"SET statement_timeout = {DB_TIMEOUT_MS}")
                cur.execute("SET default_transaction_read_only = ON")
            conn.commit()
            logging.info("✅ Odoo connection established (attempt %d)", attempt)
            yield conn
            return
        except psycopg2.OperationalError as exc:
            if conn:
                conn.close()
                conn = None
            if attempt < MAX_RETRIES:
                wait = RETRY_BASE_SECS * (2 ** (attempt - 1))
                logging.warning("⚠️  Odoo connection failed (attempt %d): %s — retry in %ds", attempt, exc, wait)
                time.sleep(wait)
            else:
                raise AirflowException(f"Odoo connection failed after {MAX_RETRIES} attempts: {exc}") from exc
        finally:
            if conn:
                conn.close()


# ─────────────────────────────────────────────────────────────
# METADATA HELPERS — WATERMARKS & LOGS
# ─────────────────────────────────────────────────────────────


def _dw() -> PostgresHook:
    return PostgresHook(postgres_conn_id=DW_CONN_ID)


def get_watermark(table_name: str) -> dict | None:
    result = _dw().get_first(
        "SELECT last_extracted_date, last_extraction_id, total_rows, consecutive_failures "
        "FROM metadata.table_watermarks WHERE table_name = %s",
        parameters=[table_name],
    )
    if not result:
        return None
    return {
        "last_date":   result[0],
        "last_id":     result[1],
        "total_rows":  result[2],
        "failures":    result[3],
    }


def update_watermark(table_name: str, last_date, total_rows: int, success: bool = True) -> None:
    if success:
        _dw().run(
            """
            INSERT INTO metadata.table_watermarks
                (table_name, last_extracted_date, total_rows, last_successful_extraction,
                 consecutive_failures, updated_at)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP, 0, CURRENT_TIMESTAMP)
            ON CONFLICT (table_name) DO UPDATE SET
                last_extracted_date     = EXCLUDED.last_extracted_date,
                total_rows              = EXCLUDED.total_rows,
                last_successful_extraction = CURRENT_TIMESTAMP,
                consecutive_failures    = 0,
                updated_at              = CURRENT_TIMESTAMP
            """,
            parameters=[table_name, last_date, total_rows],
        )
    else:
        _dw().run(
            """
            INSERT INTO metadata.table_watermarks
                (table_name, last_extracted_date, total_rows, consecutive_failures, updated_at)
            VALUES (%s, CURRENT_TIMESTAMP, %s, 1, CURRENT_TIMESTAMP)
            ON CONFLICT (table_name) DO UPDATE SET
                consecutive_failures = COALESCE(table_watermarks.consecutive_failures, 0) + 1,
                updated_at           = CURRENT_TIMESTAMP
            """,
            parameters=[table_name, total_rows],
        )


def log_extraction(
    table_name: str,
    extraction_type: str,
    rows_extracted: int,
    rows_inserted: int,
    rows_updated: int,
    status: str,
    last_date=None,
    error: str | None = None,
) -> None:
    _dw().run(
        """
        INSERT INTO metadata.extraction_log
            (table_name, extraction_type, start_time, end_time, last_extracted_date,
             rows_extracted, rows_inserted, rows_updated, status, error_message)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s)
        """,
        parameters=[
            table_name, extraction_type, timezone.utcnow(), last_date,
            rows_extracted, rows_inserted, rows_updated, status, error,
        ],
    )


# ─────────────────────────────────────────────────────────────
# FULL-LOAD DECISION LOGIC
# ─────────────────────────────────────────────────────────────


def needs_full_load(table_name: str) -> bool:
    """
    Returns True when a full reload is required:
      - Table does not yet exist in the DW
      - No watermark record found
      - 3+ consecutive failures
      - 30+ days since last full load
    """
    dw = _dw()

    table_exists = dw.get_first(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = 'raw' AND table_name = %s)",
        parameters=[table_name],
    )[0]

    if not table_exists:
        logging.info("🆕 %s not found in DW → FULL LOAD", table_name)
        return True

    wm = get_watermark(table_name)
    if not wm or not wm["last_date"]:
        logging.info("📝 No watermark for %s → FULL LOAD", table_name)
        return True

    if (wm["failures"] or 0) >= 3:
        logging.warning("🔄 %s has %d consecutive failures → FULL LOAD", table_name, wm["failures"])
        return True

    last_full = dw.get_first(
        "SELECT last_full_load FROM metadata.table_watermarks WHERE table_name = %s",
        parameters=[table_name],
    )
    if last_full and last_full[0]:
        days_since = (timezone.utcnow() - last_full[0]).days
        if days_since >= 30:
            logging.info("🔄 %s: %d days since last full load → FULL LOAD", table_name, days_since)
            return True

    logging.info("📈 %s: incremental mode (last extract: %s)", table_name, wm["last_date"])
    return False


# ─────────────────────────────────────────────────────────────
# DATAFRAME HELPERS — TYPE SAFETY
# ─────────────────────────────────────────────────────────────


def _get_target_types(table_name: str) -> dict[str, str]:
    """Read column types from information_schema to enable safe casting."""
    rows = _dw().get_records(
        """
        SELECT column_name,
               CASE
                 WHEN data_type = 'character varying' AND character_maximum_length IS NOT NULL
                   THEN 'varchar(' || character_maximum_length || ')'
                 WHEN data_type = 'numeric' AND numeric_precision IS NOT NULL
                   THEN 'numeric(' || numeric_precision || ',' || COALESCE(numeric_scale, 0) || ')'
                 WHEN data_type = 'ARRAY' THEN udt_name
                 ELSE COALESCE(udt_name, data_type)
               END AS pg_type
        FROM information_schema.columns
        WHERE table_schema = 'raw' AND table_name = %s
        ORDER BY ordinal_position
        """,
        parameters=[table_name],
    )
    return {r[0]: r[1] for r in rows}


def _sanitize(df: pd.DataFrame) -> pd.DataFrame:
    """Replace non-PostgreSQL-friendly values and remove infinities."""
    df = df.copy()
    bad_strings = {"nan", "None", "<NA>", "NaN", ""}
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].replace(bad_strings, None)
    numeric_cols = df.select_dtypes(include="number").columns
    df[numeric_cols] = df[numeric_cols].replace([np.inf, -np.inf], None)
    return df


def _cast_to_pg_types(df: pd.DataFrame, type_map: dict[str, str]) -> pd.DataFrame:
    """Align pandas dtypes with the target PostgreSQL column types."""
    df = df.copy()
    for col, pg_type in type_map.items():
        if col not in df.columns:
            continue
        t = pg_type.lower()
        try:
            if t in ("integer", "int4", "smallint", "int2", "bigint", "int8"):
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif t in ("double precision", "float8", "real", "float4"):
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
            elif t.startswith(("numeric", "decimal")):
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif t.startswith("varchar") or t in ("text", "character varying"):
                df[col] = df[col].astype(str).replace({"nan": None, "<NA>": None})
            elif t in ("boolean", "bool"):
                df[col] = df[col].astype("boolean")
            elif t in ("timestamp", "timestamp without time zone", "timestamptz"):
                df[col] = pd.to_datetime(df[col], errors="coerce")
            elif t == "date":
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
            elif t in ("json", "jsonb"):
                df[col] = df[col].apply(
                    lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
                )
        except Exception as exc:
            logging.warning("⚠️  Could not cast column %s to %s: %s", col, pg_type, exc)
    return df


def _clean_dataframe(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Normalize column names, serialize JSON fields, add lineage columns."""
    df.columns = [c.replace(" ", "_").replace("-", "_").lower() for c in df.columns]

    # Odoo stores translatable text fields as JSON dicts — serialize them
    json_text_tables = {"crm_lost_reason", "crm_stage", "crm_team", "res_company"}
    if table_name in json_text_tables:
        for col in df.select_dtypes(include="object").columns:
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)

    df["_extracted_at"]      = timezone.utcnow()
    df["_source_connection"] = ODOO_CONN_ID   # connection ID, not the actual host
    return df


# ─────────────────────────────────────────────────────────────
# UPSERT — type-safe, uses COPY for speed
# ─────────────────────────────────────────────────────────────


def upsert_dataframe(df: pd.DataFrame, table_name: str) -> tuple[int, int]:
    """
    Upsert a DataFrame into raw.<table_name> using:
      1. COPY into a temp table (fast bulk insert)
      2. INSERT ... ON CONFLICT (id) DO UPDATE (atomic upsert)

    Returns (inserts, updates).
    """
    if df.empty:
        return 0, 0

    dw   = _dw()
    temp = f"_tmp_{table_name}_{datetime.now().strftime('%H%M%S%f')}"
    types = _get_target_types(table_name)

    df_clean = _cast_to_pg_types(_sanitize(df), types)

    conn   = dw.get_conn()
    cursor = conn.cursor()
    try:
        # Create temp table mirroring the target schema
        if types:
            cursor.execute(f"CREATE TEMP TABLE {temp} AS SELECT * FROM raw.{table_name} WHERE 1=0")
        else:
            # Bootstrap: target table may not exist yet on first run
            engine = dw.get_sqlalchemy_engine()
            df_clean.head(0).to_sql(table_name, engine, schema="raw", if_exists="replace", index=False)
            cursor.execute(f"CREATE TEMP TABLE {temp} AS SELECT * FROM raw.{table_name} WHERE 1=0")

        # COPY into temp (faster than multi-row INSERT)
        buf = StringIO()
        df_clean.to_csv(buf, sep="\t", header=False, index=False, na_rep="\\N")
        buf.seek(0)
        cursor.copy_expert(f"COPY {temp} FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')", buf)

        # Build SET clause with explicit casts for type safety
        set_clause = ", ".join(
            f"{c} = EXCLUDED.{c}::{types[c]}" if c in types else f"{c} = EXCLUDED.{c}"
            for c in df_clean.columns
            if c != "id"
        )
        cols = ", ".join(df_clean.columns)

        cursor.execute(
            f"""
            WITH result AS (
                INSERT INTO raw.{table_name} ({cols})
                SELECT {cols} FROM {temp}
                ON CONFLICT (id) DO UPDATE SET {set_clause}
                RETURNING (xmax = 0) AS is_insert
            )
            SELECT
                COUNT(*) FILTER (WHERE is_insert)  AS inserts,
                COUNT(*) FILTER (WHERE NOT is_insert) AS updates
            FROM result
            """
        )
        inserts, updates = cursor.fetchone()
        cursor.execute(f"DROP TABLE IF EXISTS {temp}")
        conn.commit()
        return inserts or 0, updates or 0

    except Exception:
        conn.rollback()
        cursor.execute(f"DROP TABLE IF EXISTS {temp}")
        conn.commit()
        raise
    finally:
        cursor.close()
        conn.close()


# ─────────────────────────────────────────────────────────────
# QUALITY GATES
# ─────────────────────────────────────────────────────────────

QUALITY_THRESHOLDS = {
    "max_row_diff_pct":       5.0,
    "min_unique_id_ratio":    0.98,
    "max_null_pct_critical":  20.0,
}

CRITICAL_COLUMNS = {
    "crm_lead":   ["name"],
    "sale_order": ["name", "state"],
    "res_partner": ["name"],
}


def _run_quality_gates(table_name: str, source_count: int) -> None:
    dw = _dw()

    dw_count = dw.get_first(f"SELECT COUNT(*) FROM raw.{table_name}")[0] or 0
    if source_count > 0:
        diff_pct = abs(dw_count - source_count) / source_count * 100
        if diff_pct > QUALITY_THRESHOLDS["max_row_diff_pct"]:
            raise DataQualityError(
                f"Row count mismatch for {table_name}: "
                f"source={source_count}, dw={dw_count} ({diff_pct:.1f}% diff > threshold)"
            )

    unique_ids = dw.get_first(f"SELECT COUNT(DISTINCT id) FROM raw.{table_name}")[0] or 0
    if dw_count > 0:
        ratio = unique_ids / dw_count
        if ratio < QUALITY_THRESHOLDS["min_unique_id_ratio"]:
            raise DataQualityError(f"Duplicate IDs in {table_name}: uniqueness ratio={ratio:.3f}")

    for col in CRITICAL_COLUMNS.get(table_name, []):
        null_pct = dw.get_first(
            f"SELECT COUNT(*) FILTER (WHERE {col} IS NULL) * 100.0 / NULLIF(COUNT(*), 0) FROM raw.{table_name}"
        )[0] or 0.0
        if null_pct > QUALITY_THRESHOLDS["max_null_pct_critical"]:
            raise DataQualityError(f"{table_name}.{col}: {null_pct:.1f}% nulls exceed threshold")

    logging.info("✅ Quality gates passed for %s", table_name)


# ─────────────────────────────────────────────────────────────
# CORE EXTRACTION FUNCTIONS
# ─────────────────────────────────────────────────────────────


def extract_and_load(table_name: str) -> None:
    """
    Entry point for standard tables.
    Decides full vs. incremental, then delegates.
    """
    if needs_full_load(table_name):
        _full_load(table_name)
    else:
        _incremental_load(table_name)


def _full_load(table_name: str) -> None:
    """Drop & reload entire source table in CHUNK_SIZE batches."""
    date_field = TABLE_CONFIG[table_name]["date_field"]
    dw = _dw()

    with odoo_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            total = cur.fetchone()[0]

    if total == 0:
        logging.warning("⚠️  %s is empty in Odoo — skipping", table_name)
        return

    logging.info("🔄 Full load: %s (%d rows)", table_name, total)
    dw.run(f"DROP TABLE IF EXISTS raw.{table_name} CASCADE")

    offset     = 0
    max_date   = None
    first_batch = True

    while offset < total:
        with odoo_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM {table_name} ORDER BY id LIMIT %s OFFSET %s",
                    (CHUNK_SIZE, offset),
                )
                cols = [d[0] for d in cur.description]
                rows = cur.fetchall()

        if not rows:
            break

        df = pd.DataFrame(rows, columns=cols)
        df = _clean_dataframe(df, table_name)

        if first_batch:
            engine = dw.get_sqlalchemy_engine()
            df.head(0).to_sql(table_name, engine, schema="raw", if_exists="replace", index=False)
            first_batch = False

        engine = dw.get_sqlalchemy_engine()
        df.to_sql(table_name, engine, schema="raw", if_exists="append", index=False,
                  method="multi", chunksize=1_000)

        if date_field in df.columns:
            chunk_max = df[date_field].max()
            if pd.notna(chunk_max) and (max_date is None or chunk_max > max_date):
                max_date = chunk_max

        offset += CHUNK_SIZE
        logging.info("  📦 full load %s: %d / %d", table_name, min(offset, total), total)

    with odoo_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            source_count = cur.fetchone()[0]
    _run_quality_gates(table_name, source_count)

    dw.run(
        f"""
        INSERT INTO metadata.table_watermarks
            (table_name, last_extracted_date, total_rows, last_full_load,
             last_successful_extraction, consecutive_failures, updated_at)
        VALUES ('{table_name}', '{max_date}', {total}, CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP, 0, CURRENT_TIMESTAMP)
        ON CONFLICT (table_name) DO UPDATE SET
            last_full_load = CURRENT_TIMESTAMP,
            last_extracted_date = EXCLUDED.last_extracted_date,
            total_rows = EXCLUDED.total_rows,
            last_successful_extraction = CURRENT_TIMESTAMP,
            consecutive_failures = 0,
            updated_at = CURRENT_TIMESTAMP
        """
    )
    log_extraction(table_name, "full", total, total, 0, "completed", max_date)
    logging.info("✅ Full load complete: %s (%d rows)", table_name, total)


def _incremental_load(table_name: str) -> None:
    """
    Extract only rows modified since the last watermark.
    Processes data in CHUNK_SIZE batches with per-chunk checkpointing.
    On quality gate failure: rollback the latest extraction batch.
    """
    date_field = TABLE_CONFIG[table_name]["date_field"]
    wm         = get_watermark(table_name)
    last_date  = wm["last_date"]
    dw         = _dw()

    with odoo_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE {date_field} > %s",
                (last_date,),
            )
            modified_count = cur.fetchone()[0]

    if modified_count == 0:
        logging.info("✅ No changes in %s since %s", table_name, last_date)
        update_watermark(table_name, last_date, wm.get("total_rows", 0), success=True)
        return

    logging.info("📈 Incremental load: %s — %d rows since %s", table_name, modified_count, last_date)

    total_inserts = 0
    total_updates = 0
    max_date      = last_date
    num_chunks    = math.ceil(modified_count / CHUNK_SIZE)

    with odoo_connection() as conn:
        with conn.cursor() as cur:
            for chunk_idx in range(num_chunks):
                offset = chunk_idx * CHUNK_SIZE
                cur.execute(
                    f"""
                    SELECT * FROM {table_name}
                    WHERE {date_field} > %s
                    ORDER BY {date_field}, id
                    LIMIT %s OFFSET %s
                    """,
                    (last_date, CHUNK_SIZE, offset),
                )
                cols = [d[0] for d in cur.description]
                rows = cur.fetchall()

                if not rows:
                    break

                df = pd.DataFrame(rows, columns=cols)
                df = _clean_dataframe(df, table_name)

                ins, upd = upsert_dataframe(df, table_name)
                total_inserts += ins
                total_updates += upd

                if date_field in df.columns:
                    chunk_max = df[date_field].max()
                    if pd.notna(chunk_max) and chunk_max > max_date:
                        max_date = chunk_max

                # Checkpoint every 5 chunks
                if (chunk_idx + 1) % 5 == 0:
                    update_watermark(
                        table_name,
                        max_date,
                        (wm.get("total_rows") or 0) + total_inserts,
                        success=True,
                    )
                    logging.info(
                        "  💾 checkpoint %s: chunk %d/%d", table_name, chunk_idx + 1, num_chunks
                    )

    # Blocking quality gates — rollback on failure
    try:
        with odoo_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                source_count = cur.fetchone()[0]
        _run_quality_gates(table_name, source_count)
    except DataQualityError as exc:
        logging.error("❌ Quality gate failed for %s: %s — rolling back", table_name, exc)
        dw.run(
            f"""
            DELETE FROM raw.{table_name}
            WHERE _extracted_at = (SELECT MAX(_extracted_at) FROM raw.{table_name})
            """
        )
        update_watermark(table_name, last_date, wm.get("total_rows", 0), success=False)
        log_extraction(table_name, "incremental", modified_count, 0, 0, "quality_failed", error=str(exc))
        raise

    update_watermark(
        table_name,
        max_date,
        (wm.get("total_rows") or 0) + total_inserts,
        success=True,
    )
    log_extraction(table_name, "incremental", modified_count, total_inserts, total_updates, "completed", max_date)
    logging.info("✅ Incremental done: %s — +%d rows, ~%d updated", table_name, total_inserts, total_updates)


def extract_heavy_table(table_name: str) -> None:
    """
    Entry point for heavy tables (mail_message, mail_tracking_value).
    Uses ID-range chunking to avoid cursor timeouts on millions of rows.
    """
    if needs_full_load(table_name):
        _heavy_full_load(table_name)
    else:
        _heavy_incremental_load(table_name)


def _heavy_full_load(table_name: str) -> None:
    """Full load of heavy tables using ID-range chunks instead of OFFSET."""
    date_field = TABLE_CONFIG[table_name]["date_field"]
    dw = _dw()

    with odoo_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT MIN(id), MAX(id), COUNT(*) FROM {table_name} WHERE id IS NOT NULL"
            )
            min_id, max_id, total = cur.fetchone()

    if not total:
        return

    logging.info("🔄 Heavy full load: %s — %d rows (IDs %d–%d)", table_name, total, min_id, max_id)
    dw.run(f"DROP TABLE IF EXISTS raw.{table_name} CASCADE")

    id_range    = max_id - min_id + 1
    chunk_ids   = math.ceil(id_range / HEAVY_CHUNKS)
    first_chunk = True

    for chunk_num in range(HEAVY_CHUNKS):
        start_id = min_id + chunk_num * chunk_ids
        end_id   = min(start_id + chunk_ids - 1, max_id)

        with odoo_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM {table_name} WHERE id BETWEEN %s AND %s ORDER BY id",
                    (start_id, end_id),
                )
                cols = [d[0] for d in cur.description]
                rows = cur.fetchall()

        if not rows:
            continue

        df = pd.DataFrame(rows, columns=cols)
        df = _clean_dataframe(df, table_name)

        if first_chunk:
            engine = dw.get_sqlalchemy_engine()
            df.head(0).to_sql(table_name, engine, schema="raw", if_exists="replace", index=False)
            first_chunk = False

        engine = dw.get_sqlalchemy_engine()
        df.to_sql(table_name, engine, schema="raw", if_exists="append", index=False,
                  method="multi", chunksize=1_000)
        logging.info("  📦 heavy chunk %d/%d for %s (%d rows)", chunk_num + 1, HEAVY_CHUNKS, table_name, len(df))

    max_date = dw.get_first(
        f"SELECT MAX({date_field}) FROM raw.{table_name} WHERE {date_field} IS NOT NULL"
    )[0]
    dw.run(
        f"""
        INSERT INTO metadata.table_watermarks
            (table_name, last_extracted_date, total_rows, last_full_load,
             last_successful_extraction, consecutive_failures, updated_at)
        VALUES ('{table_name}', '{max_date}', {total}, CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP, 0, CURRENT_TIMESTAMP)
        ON CONFLICT (table_name) DO UPDATE SET
            last_full_load = CURRENT_TIMESTAMP,
            last_extracted_date = EXCLUDED.last_extracted_date,
            total_rows = EXCLUDED.total_rows,
            consecutive_failures = 0, updated_at = CURRENT_TIMESTAMP
        """
    )
    log_extraction(table_name, "full_heavy", total, total, 0, "completed", max_date)


def _heavy_incremental_load(table_name: str) -> None:
    """Incremental load for heavy tables — watermark + ID-range chunks."""
    date_field = TABLE_CONFIG[table_name]["date_field"]
    wm         = get_watermark(table_name)
    last_date  = wm["last_date"]
    dw         = _dw()

    with odoo_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE {date_field} > %s", (last_date,)
            )
            modified_count = cur.fetchone()[0]

    if modified_count == 0:
        logging.info("✅ No changes in heavy table %s", table_name)
        return

    chunk_size = math.ceil(modified_count / HEAVY_CHUNKS)

    for chunk_num in range(HEAVY_CHUNKS):
        offset = chunk_num * chunk_size
        with odoo_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT * FROM {table_name}
                    WHERE {date_field} > %s
                    ORDER BY {date_field}, id
                    LIMIT %s OFFSET %s
                    """,
                    (last_date, chunk_size, offset),
                )
                cols = [d[0] for d in cur.description]
                rows = cur.fetchall()

        if not rows:
            break
        df = pd.DataFrame(rows, columns=cols)
        df = _clean_dataframe(df, table_name)
        ins, upd = upsert_dataframe(df, table_name)
        logging.info("  📦 heavy incremental chunk %d/%d: +%d, ~%d", chunk_num + 1, HEAVY_CHUNKS, ins, upd)

    max_date = dw.get_first(
        f"SELECT MAX({date_field}) FROM raw.{table_name} WHERE {date_field} IS NOT NULL"
    )[0]
    update_watermark(table_name, max_date, (wm.get("total_rows") or 0), success=True)
    log_extraction(table_name, "incremental_heavy", modified_count, 0, 0, "completed", max_date)


# ─────────────────────────────────────────────────────────────
# INFRASTRUCTURE SETUP
# ─────────────────────────────────────────────────────────────


def create_infrastructure() -> None:
    _dw().run(
        """
        CREATE SCHEMA IF NOT EXISTS raw;
        CREATE SCHEMA IF NOT EXISTS staging;
        CREATE SCHEMA IF NOT EXISTS dimensions;
        CREATE SCHEMA IF NOT EXISTS facts;
        CREATE SCHEMA IF NOT EXISTS metadata;

        CREATE TABLE IF NOT EXISTS metadata.table_watermarks (
            table_name                VARCHAR(255) PRIMARY KEY,
            last_extracted_date       TIMESTAMP    NOT NULL,
            last_extraction_id        BIGINT,
            total_rows                BIGINT,
            last_full_load            TIMESTAMP,
            last_successful_extraction TIMESTAMP,
            consecutive_failures      INTEGER      DEFAULT 0,
            updated_at                TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS metadata.extraction_log (
            id               SERIAL PRIMARY KEY,
            table_name       VARCHAR(255)  NOT NULL,
            extraction_type  VARCHAR(50)   NOT NULL,
            start_time       TIMESTAMP     NOT NULL,
            end_time         TIMESTAMP,
            last_extracted_date TIMESTAMP,
            rows_extracted   INTEGER,
            rows_inserted    INTEGER,
            rows_updated     INTEGER,
            status           VARCHAR(50),
            error_message    TEXT,
            created_at       TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_extraction_log_table
            ON metadata.extraction_log(table_name, status);
        """
    )
    logging.info("✅ DW infrastructure ready")


def check_data_quality() -> None:
    """Post-ingestion summary quality report (non-blocking)."""
    dw = _dw()
    logging.info("=" * 70)
    logging.info("📋 DATA QUALITY REPORT")
    logging.info("=" * 70)
    for table in STANDARD_TABLES + HEAVY_TABLES:
        try:
            count = dw.get_first(f"SELECT COUNT(*) FROM raw.{table}")[0]
            unique = dw.get_first(f"SELECT COUNT(DISTINCT id) FROM raw.{table}")[0]
            status = "✅" if count == unique else "⚠️ "
            logging.info("%s raw.%-30s  rows=%-8d  unique_ids=%d", status, table, count, unique)
        except Exception as exc:
            logging.error("❌ raw.%s: %s", table, exc)
    logging.info("=" * 70)


def generate_stats() -> None:
    """Log extraction statistics for the last 7 days."""
    rows = _dw().get_records(
        """
        SELECT table_name,
               COUNT(*)                                          AS runs,
               SUM(rows_extracted)                              AS total_rows,
               AVG(EXTRACT(EPOCH FROM (end_time - start_time))) AS avg_secs,
               SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS successes
        FROM metadata.extraction_log
        WHERE start_time >= CURRENT_TIMESTAMP - INTERVAL '7 days'
        GROUP BY table_name
        ORDER BY total_rows DESC NULLS LAST
        """
    )
    logging.info("📈 7-DAY EXTRACTION STATS")
    for r in rows:
        rate = r[4] / r[1] * 100 if r[1] else 0
        logging.info("  %-30s  rows=%-8d  runs=%d  avg=%.1fs  success=%.0f%%",
                     r[0], r[2] or 0, r[1], r[3] or 0, rate)


def cleanup_old_logs() -> None:
    _dw().run(
        "DELETE FROM metadata.extraction_log WHERE created_at < CURRENT_TIMESTAMP - INTERVAL '30 days'"
    )
    logging.info("🧹 Old extraction logs purged (>30 days)")


# ─────────────────────────────────────────────────────────────
# DAG TASK DEFINITION
# ─────────────────────────────────────────────────────────────

with dag:

    # ── Infrastructure ────────────────────────────────────────
    t_infra = PythonOperator(
        task_id="create_infrastructure",
        python_callable=create_infrastructure,
    )

    # ── Standard tables (parallel, default pool) ──────────────
    with TaskGroup("ingest_standard_tables", tooltip="Watermark-based incremental extraction") as tg_standard:
        standard_tasks = [
            PythonOperator(
                task_id=f"ingest_{table}",
                python_callable=extract_and_load,
                op_kwargs={"table_name": table},
                pool="default_pool",
                retries=3,
                retry_delay=timedelta(minutes=5),
            )
            for table in STANDARD_TABLES
        ]

    # ── Heavy tables (chunked, limited pool) ──────────────────
    with TaskGroup("ingest_heavy_tables", tooltip="ID-range chunked extraction for large tables") as tg_heavy:
        heavy_tasks = [
            PythonOperator(
                task_id=f"ingest_{table}",
                python_callable=extract_heavy_table,
                op_kwargs={"table_name": table},
                pool="heavy_tables_pool",
                retries=3,
                retry_delay=timedelta(minutes=10),
            )
            for table in HEAVY_TABLES
        ]

    # ── Post-ingestion checks (all_done — run even on partial failure) ──
    with TaskGroup("post_ingestion_checks", tooltip="Quality report, stats, cleanup") as tg_checks:
        t_quality = PythonOperator(
            task_id="check_data_quality",
            python_callable=check_data_quality,
            trigger_rule="all_done",
        )
        t_stats = PythonOperator(
            task_id="generate_stats",
            python_callable=generate_stats,
            trigger_rule="all_done",
        )
        t_cleanup = PythonOperator(
            task_id="cleanup_old_logs",
            python_callable=cleanup_old_logs,
            trigger_rule="all_done",
        )

    # ── Dependencies ──────────────────────────────────────────
    t_infra >> [tg_standard, tg_heavy] >> tg_checks