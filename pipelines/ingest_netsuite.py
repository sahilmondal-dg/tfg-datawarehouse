import jaydebeapi
from utils import load_config, get_env
from pipeline_template import run_pipeline


def connect(config):
    """
    Build JDBC URL from config fields (host, port, account_id, role).
    Auth credentials are read from env vars — never hardcoded.
    """
    c = config["connection"]
    jdbc_url = (
        f"jdbc:ns://{c['host']}:{c['port']};"
        f"AccountID={c['account_id']};RoleID={c['role']}"
    )
    return jaydebeapi.connect(
        get_env("NETSUITE_DRIVER"),
        jdbc_url,
        [get_env("NETSUITE_USER"), get_env("NETSUITE_PASSWORD")]
    )


def extract(conn, table_cfg, last_run, dry_run):
    """
    SuiteQL extraction with offset-based pagination (5,000 rows per page).
    Query is defined in config — {last_run} and {offset} are substituted here.
    Dry-run: prints the first-page query and returns a dummy row (no DB call).
    """
    page_size = 5000
    offset = 0
    all_rows = []

    while True:
        query = table_cfg["query"].format(last_run=last_run, offset=offset)

        if dry_run:
            print(f"[DRY RUN] table={table_cfg['name']} offset={offset}")
            print(f"[DRY RUN] query:\n{query}")
            return [{"dummy": 1}]

        cursor = conn.cursor()
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        page = [dict(zip(columns, row)) for row in rows]

        all_rows.extend(page)

        if len(page) < page_size:
            break

        offset += page_size

    return all_rows


def main():
    config = load_config("netsuite")
    dry_run = True  # flip to False once JDBC driver and credentials are available

    conn = None if dry_run else connect(config)

    for table in config["tables"]:
        run_pipeline("netsuite", table, extract, conn, dry_run=dry_run)


if __name__ == "__main__":
    main()
