"""Read back what the producer wrote and summarise it.

Scheduled on the Asset rather than on a clock: this runs because the producer
declared it wrote, not because a time arrived. The URI below has to resolve to
the same string the producer builds — same expression, same environment, so it
does.
"""

from __future__ import annotations

import csv
import io
import os

import pendulum
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import Asset, dag, task

BUCKET = os.environ.get("AIRFLOW_VAR_S3_BUCKET", "lakehouse")
PREFIX = "curated/meter_readings"
CONN = "s3_logs"

METER_READINGS = Asset(f"s3://{BUCKET}/{PREFIX}/")


@dag(
    schedule=[METER_READINGS],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["assets", "demo"],
)
def asset_consumer():
    @task
    def summarise() -> None:
        hook = S3Hook(aws_conn_id=CONN)

        keys = hook.list_keys(bucket_name=BUCKET, prefix=f"{PREFIX}/") or []
        batches = sorted(k for k in keys if k.endswith(".csv"))
        if not batches:
            # Loud on purpose. This DAG only runs because the producer said it
            # wrote something, so an empty prefix means the write failed
            # quietly — and a task that shrugged and returned would hide it.
            raise ValueError(
                f"nothing under s3://{BUCKET}/{PREFIX}/, but the producer "
                "reported success — check the producer's task log"
            )

        latest = batches[-1]
        rows = list(csv.DictReader(io.StringIO(hook.read_key(key=latest, bucket_name=BUCKET))))
        total = sum(float(row["kwh"]) for row in rows)

        print(f"{latest}: {len(rows)} readings, {total:.3f} kWh total")
        print(f"{len(batches)} batches in the prefix so far")

    summarise()


asset_consumer()
