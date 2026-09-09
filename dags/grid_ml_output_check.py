"""Watch that the grid-loss ML job is actually still producing output.

WHY THIS EXISTS. The Spark job writes Parquet to object storage and nothing
downstream notices when it stops. Its tables in Trino are external, so they
keep answering queries from the last files written — a run that failed, or one
that found no records in Kafka and exited clean, leaves yesterday's numbers in
place and every dashboard still looks healthy. The failure is silent by
construction.

So this checks the one thing that cannot be faked: how old the newest object
under each output prefix is.

Deliberately reads only S3 object metadata, not the Parquet itself. Opening the
files would need pyarrow in the Airflow image, which is not something to depend
on for a monitor — and the question here is "did it run", which a timestamp
answers completely.

Bucket comes from the chart (AIRFLOW_VAR_S3_BUCKET), so this DAG is portable
between installs whose buckets are named differently. Credentials come from the
s3_logs connection, which every Airflow role already has.
"""

from __future__ import annotations

import os

import pendulum
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task

BUCKET = os.environ.get("AIRFLOW_VAR_S3_BUCKET", "lakehouse")
CONN = "s3_logs"

# The three tables the job writes. Named individually rather than scanning
# `grid/`, so a table that disappears entirely is caught too — an empty prefix
# is a finding, not an absence of one.
OUTPUTS = ["grid/grid_losses", "grid/meter_anomalies", "grid/zone_summary"]

# The rerun CronJob is hourly. Three hours leaves room for a slow run and one
# missed cycle without crying wolf; a second consecutive miss is a real fault.
MAX_AGE_HOURS = 3


@dag(
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["grid", "monitoring"],
)
def grid_ml_output_check():
    @task
    def check_freshness() -> None:
        client = S3Hook(aws_conn_id=CONN).get_conn()
        now = pendulum.now("UTC")
        stale: list[str] = []

        for prefix in OUTPUTS:
            newest = None
            objects = 0

            # Paginated on purpose: a prefix accumulates a file per run, and
            # list_objects_v2 stops at 1000 without saying so.
            token = None
            while True:
                kwargs = {"Bucket": BUCKET, "Prefix": f"{prefix}/"}
                if token:
                    kwargs["ContinuationToken"] = token
                page = client.list_objects_v2(**kwargs)

                for obj in page.get("Contents", []):
                    objects += 1
                    ts = pendulum.instance(obj["LastModified"])
                    if newest is None or ts > newest:
                        newest = ts

                if not page.get("IsTruncated"):
                    break
                token = page.get("NextContinuationToken")

            if newest is None:
                print(f"{prefix}: EMPTY — no objects at all")
                stale.append(f"{prefix} (empty)")
                continue

            age = (now - newest).total_hours()
            state = "ok" if age <= MAX_AGE_HOURS else "STALE"
            print(f"{prefix}: {objects} objects, newest {age:.1f}h old — {state}")
            if age > MAX_AGE_HOURS:
                stale.append(f"{prefix} ({age:.1f}h)")

        if stale:
            # Fails the run rather than logging a warning. A warning in a task
            # log is something nobody reads; a red run is on the home screen.
            raise ValueError(
                f"grid ML output has not been refreshed within {MAX_AGE_HOURS}h: "
                + ", ".join(stale)
                + f". Check the driver: kubectl get pods | grep grid-ml, and the "
                f"CronJob grid-ml-rerun. An empty prefix usually means the job "
                f"ran but found no records in Kafka — check the NiFi flows."
            )

        print(f"All {len(OUTPUTS)} outputs fresh in s3://{BUCKET}/grid/")

    check_freshness()


grid_ml_output_check()
