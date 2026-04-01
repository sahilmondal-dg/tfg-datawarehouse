import os
import sys
import requests
from datetime import datetime, UTC
from utils import (
    load_config, get_env,
    get_last_run, update_last_run,
    land_to_onelake, write_pipeline_log, send_alert
)


def get_access_token():
    """
    OAuth2 client credentials flow.
    Returns None if ADP_TOKEN_URL / ADP_CLIENT_ID / ADP_CLIENT_SECRET are not set
    (e.g. when testing against a no-auth API like DummyJSON).
    When all three vars are present, fetches a fresh bearer token.
    """
    token_url = os.getenv("ADP_TOKEN_URL")
    client_id = os.getenv("ADP_CLIENT_ID")
    client_secret = os.getenv("ADP_CLIENT_SECRET")

    if not all([token_url, client_id, client_secret]):
        return None

    resp = requests.post(
        token_url,
        data={"grant_type": "client_credentials"},
        auth=(client_id, client_secret),
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def paginate(endpoint, params, token, dry_run, table_cfg):
    """
    Paginated extraction. All pagination and response shape details are config-driven:

        top_param  — query param name for page size  (default: "$top",  DummyJSON: "limit")
        skip_param — query param name for offset      (default: "$skip", DummyJSON: "skip")
        page_size  — rows per page                    (default: 100)
        data_key   — top-level response key for rows  (default: "data", DummyJSON: "users"/"products"/etc.)

    Dry-run: fetches first page only and logs response shape — no further pages.
    Live: pages through until a page smaller than page_size is returned.
    Authorization header is only added when a token is present.
    """
    base_url = get_env("ADP_BASE_URL")

    top_param  = table_cfg.get("top_param",  "$top")
    skip_param = table_cfg.get("skip_param", "$skip")
    page_size  = table_cfg.get("page_size",  100)
    data_key   = table_cfg.get("data_key",   "data")

    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    all_rows = []
    skip = 0

    while True:
        page_params = {top_param: page_size, skip_param: skip, **params}
        resp = requests.get(base_url + endpoint, headers=headers, params=page_params)
        resp.raise_for_status()
        page = resp.json().get(data_key, [])

        if dry_run:
            shape = list(page[0].keys()) if page else []
            nested = [k for k in shape if isinstance(page[0].get(k), (list, dict))] if page else []
            print(f"[DRY RUN] endpoint={endpoint}  rows={len(page)}  data_key={data_key}")
            print(f"[DRY RUN] keys:   {shape}")
            print(f"[DRY RUN] nested: {nested}")
            return page  # first page only

        all_rows.extend(page)
        if len(page) < page_size:
            break
        skip += page_size

    return all_rows


def separate_nested(rows, pk_field):
    """
    Split nested JSON arrays into separate tables.
    Scalar fields stay in the parent table. List-valued fields become child tables,
    each row inheriting the parent PK — no collapsing or JSON-stringifying.

    Returns:
        parent_rows  — list of dicts with only scalar fields
        child_tables — dict of {field_name: [rows]}, each row has the parent PK prepended
    """
    parent_rows = []
    child_tables = {}

    for row in rows:
        parent = {}
        for k, v in row.items():
            if isinstance(v, list):
                child_tables.setdefault(k, [])
                for item in v:
                    if isinstance(item, dict):
                        child_tables[k].append({pk_field: row.get(pk_field), **item})
                    else:
                        child_tables[k].append({pk_field: row.get(pk_field), "value": item})
            else:
                parent[k] = v
        parent_rows.append(parent)

    return parent_rows, child_tables


def main():
    config = load_config("adp")
    dry_run = "--dry-run" in sys.argv
    partition_date = datetime.now(UTC).strftime("%Y-%m-%d")
    token = None if dry_run else get_access_token()

    if dry_run:
        print("[DRY RUN MODE] First page fetched per table — nothing will be landed or committed.")

    for table_cfg in config["tables"]:
        table_name = table_cfg["name"]
        pk = table_cfg["primary_key"]
        last_run = get_last_run("adp", table_name) or "1970-01-01"

        params = {
            k: v.format(last_run=last_run)
            for k, v in table_cfg.get("params", {}).items()
        }

        try:
            rows = paginate(table_cfg["endpoint"], params, token, dry_run, table_cfg)

            if not rows:
                raise ValueError(f"No data returned from {table_cfg['endpoint']}")

            parent_rows, child_tables = separate_nested(rows, pk)

            if any(r.get(pk) is None for r in parent_rows):
                raise ValueError(f"Primary key '{pk}' has nulls in {table_name}")

            if not dry_run:
                land_to_onelake(parent_rows, "adp", table_name, partition_date)

                for child_name, child_rows in child_tables.items():
                    child_table = f"{table_name}_{child_name}"
                    land_to_onelake(child_rows, "adp", child_table, partition_date)
                    write_pipeline_log("adp", child_table, len(child_rows), "SUCCESS")

                ts_field = table_cfg.get("timestamp_field")
                if ts_field and table_cfg.get("load_type") != "full":
                    max_ts = max(
                        (r.get(ts_field) for r in parent_rows if r.get(ts_field)),
                        default=None
                    )
                    if max_ts:
                        update_last_run("adp", table_name, str(max_ts))

            write_pipeline_log("adp", table_name, len(parent_rows), "SUCCESS")

        except Exception as e:
            write_pipeline_log("adp", table_name, 0, "FAILED", str(e))
            send_alert(f"adp.{table_name} failed: {str(e)}")
            raise


if __name__ == "__main__":
    main()
