import io
import fnmatch
import pandas as pd
import paramiko
import boto3
from datetime import datetime, UTC
from utils import (
    load_config, get_env,
    get_last_run, update_last_run,
    land_to_onelake, write_pipeline_log, send_alert
)


# =============================================================================
# SFTP helpers
# =============================================================================

def sftp_connect(port):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        get_env("ADP_SFTP_HOST"),
        port=port,
        username=get_env("ADP_SFTP_USER"),
        password=get_env("ADP_SFTP_PASSWORD"),
    )
    return ssh, ssh.open_sftp()


def list_sftp_files(sftp, remote_path, pattern):
    return sorted(f for f in sftp.listdir(remote_path) if fnmatch.fnmatch(f, pattern))


def read_sftp_file(sftp, remote_path, filename):
    with sftp.open(f"{remote_path}/{filename}", "r") as f:
        return io.StringIO(f.read().decode("utf-8"))


# =============================================================================
# S3 helpers
# =============================================================================

def list_s3_files(s3, bucket, prefix, pattern):
    paginator = s3.get_paginator("list_objects_v2")
    files = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"].split("/")[-1]
            if fnmatch.fnmatch(key, pattern):
                files.append(key)
    return sorted(files)


def read_s3_file(s3, bucket, prefix, filename):
    obj = s3.get_object(Bucket=bucket, Key=f"{prefix}/{filename}")
    return io.StringIO(obj["Body"].read().decode("utf-8"))


# =============================================================================
# Core processing
# =============================================================================

def get_new_files(all_files, last_processed):
    """
    Return files not yet processed.
    Relies on ADP export filenames sorting chronologically (e.g. workers_20240115.csv).
    Confirm filename format with Zeel before enabling live runs.
    """
    if not last_processed:
        return all_files
    return [f for f in all_files if f > last_processed]


def process_table(transport_cfg, table_cfg, dry_run):
    source = "adp_csv"
    table_name = table_cfg["name"]
    pattern = table_cfg["file_pattern"]
    # last_run stores the last filename processed (not a timestamp)
    last_processed = get_last_run(source, table_name)
    partition_date = datetime.now(UTC).strftime("%Y-%m-%d")
    transport = transport_cfg["type"]

    if transport == "sftp":
        port = int(transport_cfg.get("port", 22))
        remote_path = transport_cfg["path"]
        ssh, sftp = sftp_connect(port)

        try:
            all_files = list_sftp_files(sftp, remote_path, pattern)
            new_files = get_new_files(all_files, last_processed)

            if dry_run:
                print(f"[DRY RUN] {table_name}: {len(new_files)} new file(s) found: {new_files}")
                return

            for filename in new_files:
                content = read_sftp_file(sftp, remote_path, filename)
                df = pd.read_csv(content)  # column names from file header — no renaming
                rows = df.to_dict(orient="records")
                land_to_onelake(rows, source, table_name, partition_date)
                write_pipeline_log(source, table_name, len(rows), "SUCCESS")
                update_last_run(source, table_name, filename)  # track by filename
        finally:
            sftp.close()
            ssh.close()

    elif transport == "s3":
        bucket = get_env("ADP_S3_BUCKET")
        prefix = transport_cfg["prefix"]
        s3 = boto3.client("s3")

        all_files = list_s3_files(s3, bucket, prefix, pattern)
        new_files = get_new_files(all_files, last_processed)

        if dry_run:
            print(f"[DRY RUN] {table_name}: {len(new_files)} new file(s) found: {new_files}")
            return

        for filename in new_files:
            content = read_s3_file(s3, bucket, prefix, filename)
            df = pd.read_csv(content)
            rows = df.to_dict(orient="records")
            land_to_onelake(rows, source, table_name, partition_date)
            write_pipeline_log(source, table_name, len(rows), "SUCCESS")
            update_last_run(source, table_name, filename)

    else:
        raise ValueError(f"Unknown transport type: {transport!r}. Must be 'sftp' or 's3'.")


def main():
    config = load_config("adp_csv")
    transport_cfg = config["transport"]
    # ⚠️  Keep dry_run=True until file patterns are confirmed with Zeel
    dry_run = True

    for table_cfg in config["tables"]:
        try:
            process_table(transport_cfg, table_cfg, dry_run)
        except Exception as e:
            write_pipeline_log("adp_csv", table_cfg["name"], 0, "FAILED", str(e))
            send_alert(f"adp_csv.{table_cfg['name']} failed: {str(e)}")
            raise


if __name__ == "__main__":
    main()
