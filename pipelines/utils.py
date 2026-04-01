import yaml
import json
import os
from datetime import datetime, UTC
from dotenv import load_dotenv

load_dotenv()

STATE_FILE = "state/pipeline_state.json"


# =========================
# CONFIG
# =========================
def load_config(source_name):
    with open(f"config/{source_name}.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# =========================
# ENV HELPER
# =========================
def get_env(key):
    value = os.getenv(key)
    if not value:
        raise ValueError(f"Missing environment variable: {key}")
    return value


# =========================
# INCREMENTAL STATE
# =========================
def get_last_run(source, table):
    try:
        with open(STATE_FILE, "r") as f:
            state = json.load(f)
        return state.get(source, {}).get(table)
    except Exception:
        return None


def update_last_run(source, table, last_run):
    try:
        with open(STATE_FILE, "r") as f:
            state = json.load(f)
    except Exception:
        state = {}

    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    state.setdefault(source, {})[table] = last_run

    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


# =========================
# FABRIC LANDING (Delta format → Lakehouse Tables)
# =========================
def land_to_onelake(rows, source, table, partition_date):
    """
    Writes rows as a Delta table to OneLake Lakehouse Tables section.
    Table name: bronze_{source}_{table}  (e.g. bronze_adp_users)
    Falls back to local JSONL when FABRIC_ACCOUNT_URL is not set.
    """
    account_url = os.getenv("FABRIC_ACCOUNT_URL")

    if not account_url:
        # Local fallback — used during testing before Fabric creds are available
        out_dir = os.path.join("test_output", source)
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, f"{table}_{partition_date}.jsonl")
        with open(out_path, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, default=str) + "\n")
        print(f"[LOCAL]   {out_path}  ({len(rows)} rows)")
        return

    import pandas as pd
    from deltalake import write_deltalake

    workspace_id = get_env("FABRIC_FILE_SYSTEM")
    lakehouse_id = get_env("FABRIC_LAKEHOUSE_ID")
    table_name = f"bronze_{source}_{table}"

    # abfss://<workspace-guid>@onelake.dfs.fabric.microsoft.com/<lakehouse-guid>/Tables/<name>
    delta_path = (
        f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com"
        f"/{lakehouse_id}/Tables/{table_name}"
    )

    storage_options = {
        "azure_storage_client_id": get_env("AZURE_CLIENT_ID"),
        "azure_storage_client_secret": get_env("AZURE_CLIENT_SECRET"),
        "azure_storage_tenant_id": get_env("AZURE_TENANT_ID"),
    }

    # Flatten nested dicts/lists to JSON strings so Delta can handle them
    processed = []
    for row in rows:
        processed.append({
            k: json.dumps(v, default=str) if isinstance(v, (dict, list)) else v
            for k, v in row.items()
        })

    df = pd.DataFrame(processed)
    write_deltalake(delta_path, df, mode="overwrite", storage_options=storage_options)
    print(f"[DELTA]   {delta_path}  ({len(rows)} rows)")


# =========================
# LOGGING
# =========================
def write_pipeline_log(source, table, row_count, status, error=None):
    log = {
        "source_system": source,
        "table_name": table,
        "last_run_at": datetime.now(UTC).isoformat(),
        "rows_extracted": row_count,
        "status": status,
        "error_message": error
    }
    print("[PIPELINE LOG]", log)


# =========================
# ALERT
# =========================
def send_alert(message):
    print("[ALERT]", message)