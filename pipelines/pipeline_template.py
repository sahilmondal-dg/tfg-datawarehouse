# =============================================================================
# PIPELINE TEMPLATE — pattern reference for all bronze connectors
# =============================================================================
#
# Each connector (ingest_<source>.py) implements SECTIONS 1–3 in its own file.
# SECTIONS 4–6 are fully implemented in run_pipeline() below — do not reimplement.
#
# SECTION 1 — CONFIG
#   Load the source config and iterate over tables:
#       config = load_config("source_name")     # loads config/<source_name>.yaml
#       for table in config["tables"]: ...
#
# SECTION 2 — CONNECT
#   Establish the connection (DB, API, SFTP) using config params + env var credentials:
#       conn = connect(config)                  # build JDBC URL / API client / SSH session
#       # credentials from env vars only — never hardcode secrets
#
# SECTION 3 — EXTRACT
#   Implement the extract function with this exact signature:
#       def extract(conn, table_cfg, last_run, dry_run) -> list[dict]:
#           # Build query / request using table_cfg fields
#           # Apply incremental filter:  WHERE timestamp_field > last_run
#           # Implement pagination if the source requires it
#           # Dry-run: print query/URL and return [{"dummy": 1}] — no I/O
#           # Return: flat list of dicts, one dict per row
#
# SECTIONS 4–6 — handled automatically by run_pipeline():
#   SECTION 4 — VALIDATE   assert row_count > 0; assert primary key has no nulls
#   SECTION 5 — LAND       land_to_onelake(data, source, table, partition_date)
#   SECTION 6 — LOG        write_pipeline_log(source, table, row_count, status)
#
# =============================================================================

from datetime import datetime, UTC
from utils import (
    load_config,
    land_to_onelake,
    write_pipeline_log,
    send_alert,
    get_last_run,
    update_last_run
)


def run_pipeline(source_name, table_cfg, extract_fn, conn=None, dry_run=False):
    table_name = table_cfg["name"]

    try:
        # ── SECTION 1: CONFIG ────────────────────────────────────────────────
        last_run = get_last_run(source_name, table_name) or "1970-01-01"

        # ── SECTION 3: EXTRACT ───────────────────────────────────────────────
        data = extract_fn(conn, table_cfg, last_run, dry_run)

        # ── SECTION 4: VALIDATE ──────────────────────────────────────────────
        if not data:
            raise ValueError("No data extracted")

        row_count = len(data)

        pk = table_cfg.get("primary_key")
        if pk and any(row.get(pk) is None for row in data):
            raise ValueError(f"Primary key '{pk}' has nulls")

        # ── SECTION 5: LAND ──────────────────────────────────────────────────
        partition_date = datetime.now(UTC).strftime("%Y-%m-%d")

        if not dry_run:
            land_to_onelake(data, source_name, table_name, partition_date)

            ts_field = table_cfg.get("timestamp_field")
            if ts_field:
                max_ts = max(row.get(ts_field) for row in data if row.get(ts_field))
                update_last_run(source_name, table_name, str(max_ts))

        # ── SECTION 6: LOG ───────────────────────────────────────────────────
        write_pipeline_log(source_name, table_name, row_count, "SUCCESS")

    except Exception as e:
        write_pipeline_log(source_name, table_name, 0, "FAILED", str(e))
        send_alert(f"{source_name}.{table_name} failed: {str(e)}")
        raise
