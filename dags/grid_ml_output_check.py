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

THE STALENESS THRESHOLD IS DERIVED, NOT DECLARED, and the first version of this
got that wrong. It hard-coded three hours with the comment "the rerun CronJob is
hourly" — but the chart's install form offers hourly, 6-hourly and nightly, so
that constant was an assumption about a value the installer chooses. On a
nightly install it would have fired every single day, forever, and a check that
fires on a normal day gets muted within a week and then protects nothing.

A larger constant does not fix it, it just relocates the wrongness: twelve hours
is far too lax for an hourly job and still wrong for a nightly one. The cadence
is not ours to assume, but it is not unknown either — it is written in the data.
The history prefix is append-only, one file per run, so the gaps between those
files are the pipeline's own record of how often it actually runs, whatever was
configured. Median of those gaps, doubled, plus an hour of slack.

That formula is chosen so that an hourly install lands on exactly the three
hours this DAG used before, which makes the change a no-op where it was already
right and a fix everywhere else.
"""

from __future__ import annotations

import os
import statistics

import pendulum
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task

BUCKET = os.environ.get("AIRFLOW_VAR_S3_BUCKET", "lakehouse")
CONN = "s3_logs"

# The three tables the job writes. Named individually rather than scanning
# `grid/`, so a table that disappears entirely is caught too — an empty prefix
# is a finding, not an absence of one.
OUTPUTS = ["grid/grid_losses", "grid/meter_anomalies", "grid/zone_summary"]

# Where the cadence is read from. It has to be one of the *_history locations:
# the three prefixes above are written with mode("overwrite"), so each run
# replaces them and they hold only the latest run's files — no record of when
# anything happened. The history locations are appended to, one file per run,
# which is exactly the record needed. zone_summary_history is the smallest of
# the three, so listing it is the cheapest.
CADENCE_SOURCE = "grid/zone_summary_history"

# Used when the cadence cannot be derived: history disabled in the chart, or a
# fresh install with too few runs to measure. Matches the old hard-coded value,
# which is right for the default hourly schedule.
FALLBACK_MAX_AGE_HOURS = 3.0

# Enough writes for a median to mean something. Three gaps: one slow run cannot
# move the middle value.
MIN_WRITES = 4

# A derived threshold is never allowed past this. The derivation is naturally
# resistant to an ongoing outage — a job that stopped yesterday contributes no
# new gap, it just makes `age` large — but a long stretch of erratic running
# could still inflate the median, and a monitor that quietly widens its own
# tolerance until it can no longer fail is worse than no monitor.
CEILING_HOURS = 72.0


def _timestamps(client, prefix: str) -> tuple[list, int]:
    """Every object timestamp under a prefix, plus the raw object count.

    Paginated on purpose: a prefix accumulates a file per run, and
    list_objects_v2 stops at 1000 without saying so.
    """
    stamps: list = []
    count = 0
    token = None

    while True:
        kwargs = {"Bucket": BUCKET, "Prefix": f"{prefix}/"}
        if token:
            kwargs["ContinuationToken"] = token
        page = client.list_objects_v2(**kwargs)

        for obj in page.get("Contents", []):
            count += 1
            stamps.append((obj["Key"], pendulum.instance(obj["LastModified"])))

        if not page.get("IsTruncated"):
            break
        token = page.get("NextContinuationToken")

    return stamps, count


@dag(
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["grid", "monitoring"],
)
def grid_ml_output_check():
    @task
    def derive_threshold() -> float:
        """Work out how often this install actually runs, from its own history.

        Only .parquet objects count. `_SUCCESS` is rewritten by every run, so
        it carries one timestamp that always equals the newest — harmless for
        finding the latest write, but it is not a run of its own and has no
        place among the gaps.
        """
        client = S3Hook(aws_conn_id=CONN).get_conn()
        stamps, _ = _timestamps(client, CADENCE_SOURCE)
        runs = sorted(ts for key, ts in stamps if key.endswith(".parquet"))

        if len(runs) < MIN_WRITES:
            print(
                f"{CADENCE_SOURCE}: {len(runs)} runs recorded, need {MIN_WRITES} "
                f"to measure a cadence — falling back to {FALLBACK_MAX_AGE_HOURS}h. "
                f"If history is disabled in the chart (history.enabled=false) "
                f"this is permanent, and the fallback assumes the default "
                f"hourly schedule."
            )
            return FALLBACK_MAX_AGE_HOURS

        gaps = [
            (b - a).total_seconds() / 3600.0
            for a, b in zip(runs, runs[1:])
        ]
        median = statistics.median(gaps)
        threshold = min(2.0 * median + 1.0, CEILING_HOURS)

        print(
            f"{CADENCE_SOURCE}: {len(runs)} runs, median gap {median:.2f}h "
            f"-> stale after {threshold:.1f}h"
        )
        if threshold == CEILING_HOURS:
            print(
                f"capped at the {CEILING_HOURS:.0f}h ceiling — the measured "
                f"cadence is unusually long or unusually irregular, which is "
                f"itself worth looking at."
            )
        return threshold

    @task
    def check_freshness(max_age_hours: float) -> None:
        client = S3Hook(aws_conn_id=CONN).get_conn()
        now = pendulum.now("UTC")
        stale: list[str] = []

        for prefix in OUTPUTS:
            stamps, objects = _timestamps(client, prefix)
            newest = max((ts for _, ts in stamps), default=None)

            if newest is None:
                print(f"{prefix}: EMPTY — no objects at all")
                stale.append(f"{prefix} (empty)")
                continue

            age = (now - newest).total_hours()
            state = "ok" if age <= max_age_hours else "STALE"
            print(f"{prefix}: {objects} objects, newest {age:.1f}h old — {state}")
            if age > max_age_hours:
                stale.append(f"{prefix} ({age:.1f}h)")

        if stale:
            # Fails the run rather than logging a warning. A warning in a task
            # log is something nobody reads; a red run is on the home screen.
            raise ValueError(
                f"grid ML output has not been refreshed within "
                f"{max_age_hours:.1f}h: "
                + ", ".join(stale)
                + f". That threshold was measured from this install's own run "
                f"history, not assumed — see the derive_threshold task for the "
                f"cadence it found. Check the driver: kubectl get pods | grep "
                f"grid-ml, and the CronJob grid-ml-rerun. An empty prefix "
                f"usually means the job ran but found no records in Kafka — "
                f"check the NiFi flows."
            )

        print(f"All {len(OUTPUTS)} outputs fresh in s3://{BUCKET}/grid/")

    check_freshness(derive_threshold())


grid_ml_output_check()
