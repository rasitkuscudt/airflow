"""Write a batch of meter readings to object storage, then announce it.

The bucket is deliberately not written into this file. It is answered when the
platform is installed, so the same DAG has to run unchanged on a cluster whose
bucket is called something else — the chart passes the name in as the Airflow
Variable `s3_bucket`.

Read here from the environment rather than with `Variable.get`, because this
runs at PARSE time: the Asset URI is the join key between this DAG and the
consumer, so it has to exist before either task starts. Inside a task the
idiomatic form is `Variable.get("s3_bucket")` — same value, and it reaches the
same environment variable.

No credentials either. The platform's S3 account already reaches Airflow as
the connection `s3_logs`, injected into the webserver, scheduler, DAG
processor and workers.
"""

from __future__ import annotations

import csv
import io
import os
import random

import pendulum
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import Asset, dag, task

# The default keeps both files parseable on an install with no object storage.
# Without it a missing Variable would break the pair at import, which reads as
# "the DAGs disappeared" rather than "one task cannot run".
BUCKET = os.environ.get("AIRFLOW_VAR_S3_BUCKET", "lakehouse")
PREFIX = "curated/meter_readings"
CONN = "s3_logs"

# Both DAGs parse in the same environment, so both resolve the same bucket and
# the two URIs match. That match is the only thing linking them.
METER_READINGS = Asset(f"s3://{BUCKET}/{PREFIX}/")


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["assets", "demo"],
)
def asset_producer():
    @task(outlets=[METER_READINGS])
    def publish() -> str:
        now = pendulum.now("UTC")

        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["meter_id", "read_at", "kwh"])
        for meter in range(1, 51):
            writer.writerow(
                [
                    f"M-{meter:04d}",
                    now.to_iso8601_string(),
                    round(random.uniform(0.2, 4.5), 3),
                ]
            )

        # Date first, then time, both zero-padded: the keys then sort
        # lexicographically in the order they were written, which is what lets
        # the consumer pick the newest without asking S3 for timestamps.
        key = f"{PREFIX}/{now.format('YYYY-MM-DD')}/{now.format('HHmmss')}.csv"

        S3Hook(aws_conn_id=CONN).load_string(
            string_data=buf.getvalue(),
            key=key,
            bucket_name=BUCKET,
            replace=True,
        )
        print(f"wrote 50 readings to s3://{BUCKET}/{key}")
        return key

    publish()


asset_producer()
