import yaml
import json
import os
from datetime import datetime, UTC
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential
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
# FABRIC AUTH (Service Principal)
# =========================
def _get_onelake_client() -> DataLakeServiceClient:
    credential = ClientSecretCredential(
        tenant_id=get_env("AZURE_TENANT_ID"),
        client_id=get_env("AZURE_CLIENT_ID"),
        client_secret=get_env("AZURE_CLIENT_SECRET"),
    )
    return DataLakeServiceClient(
        account_url=get_env("FABRIC_ACCOUNT_URL"),
        credential=credential
    )


# =========================
# FABRIC LANDING
# =========================
def land_to_onelake(rows, source, table, partition_date):
    """
    Lands rows to OneLake when FABRIC_ACCOUNT_URL is set.
    Falls back to local test_output/ when Fabric creds are not configured.
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

    file_system = get_env("FABRIC_FILE_SYSTEM")

    client = _get_onelake_client()
    fs = client.get_file_system_client(file_system)

    path = f"Files/bronze/source={source}/table={table}/dt={partition_date}/data.json"
    file_client = fs.get_file_client(path)

    content = "\n".join([json.dumps(row, default=str) for row in rows])
    file_client.upload_data(content, overwrite=True)

    print(f"[ONELAKE] {path}  ({len(rows)} rows)")


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