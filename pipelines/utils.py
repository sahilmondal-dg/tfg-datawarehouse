import yaml
import json
import os
import time
import requests
from datetime import datetime, UTC
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.credentials import AccessToken
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
# FABRIC AUTH (Option A — user password flow)
# =========================
class _StaticTokenCredential:
    """
    Wraps a raw token string into the Azure SDK credential interface.
    Token is fetched once per pipeline run — fresh each GitHub Actions job.
    """
    def __init__(self, token: str):
        self._token = token

    def get_token(self, *scopes, **kwargs):
        return AccessToken(self._token, int(time.time()) + 3600)


def _get_fabric_token() -> str:
    """
    Fetches a short-lived Azure AD token using username/password flow.
    Requires AZURE_TENANT_ID, FABRIC_USERNAME, FABRIC_PASSWORD in env.
    Will raise clearly if MFA is enforced on the account.
    """
    tenant_id = get_env("AZURE_TENANT_ID")
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

    payload = {
        "grant_type": "password",
        # Well-known Azure PowerShell public client ID — no app registration needed
        "client_id": "1950a258-227b-4e31-a9cf-717495945fc2",
        "username": get_env("FABRIC_USERNAME"),
        "password": get_env("FABRIC_PASSWORD"),
        "scope": "https://storage.azure.com/.default",
    }

    response = requests.post(url, data=payload, timeout=30)

    data = response.json()

    if "access_token" not in data:
        error = data.get("error", "unknown")
        description = data.get("error_description", "no description")

        # Surface MFA failure clearly rather than a cryptic SDK error
        if "AADSTS50076" in description or "AADSTS50079" in description:
            raise RuntimeError(
                "MFA is enforced on this account — Option A will not work. "
                "Either disable MFA on this account or switch to Option B (dedicated service account)."
            )

        raise RuntimeError(f"Token fetch failed [{error}]: {description}")

    return data["access_token"]


def _get_onelake_client() -> DataLakeServiceClient:
    token = _get_fabric_token()
    return DataLakeServiceClient(
        account_url=get_env("FABRIC_ACCOUNT_URL"),
        credential=_StaticTokenCredential(token)
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

    # Old static token path removed — always use user token flow now
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