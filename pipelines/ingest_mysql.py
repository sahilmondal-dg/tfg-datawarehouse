import sys
import pymysql
from utils import load_config, get_env
from pipeline_template import run_pipeline


def connect():
    return pymysql.connect(
        host=get_env("MYSQL_HOST"),
        user=get_env("MYSQL_USER"),
        password=get_env("MYSQL_PASSWORD"),
        database=get_env("MYSQL_DATABASE"),
        cursorclass=pymysql.cursors.DictCursor
    )


def extract(conn, table_cfg, last_run, dry_run):
    table = table_cfg["name"]
    ts = table_cfg["timestamp_field"]
    # table and ts are config-driven (trusted internal values), last_run from state file
    query = f"SELECT * FROM {table} WHERE {ts} > '{last_run}'"

    if dry_run:
        print("[DRY RUN]", query)
        return [{"dummy": 1}]

    cursor = conn.cursor()
    cursor.execute(query)
    return cursor.fetchall()


def main():
    # Source name is passed as CLI arg so this connector is reusable for any MySQL source.
    # Usage:  python ingest_mysql.py janit
    # Config: config/janit.yaml (table names populated after Day 1 schema discovery)
    source = sys.argv[1] if len(sys.argv) > 1 else "mysql"
    config = load_config(source)
    dry_run = True  # flip to False once MySQL credentials are available

    conn = None if dry_run else connect()

    for table in config["tables"]:
        run_pipeline(source, table, extract, conn, dry_run=dry_run)


if __name__ == "__main__":
    main()
