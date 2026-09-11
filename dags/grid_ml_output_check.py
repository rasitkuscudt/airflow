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

# Used only when NO gap can be measured at all — history disabled in the chart,
# or a brand-new install with a single run recorded. Deliberately generous
# enough to cover one nightly cycle with slack, because the alternative is
# assuming a cadence, and the wrong assumption here is the expensive one: a
# monitor that is briefly too lax costs a day of blindness on a fresh install,
# while one that cries wolf every day is muted within a week and then protects
# nothing for good.
FALLBACK_MAX_AGE_HOURS = 30.0

# Enough gaps for a median to be robust — one slow run cannot move the middle
# value. Below this the gaps that exist are still used (see derive_threshold);
# what changes is how they are combined, not whether they are trusted.
ROBUST_GAPS = 3

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

        gaps = [
            (b - a).total_seconds() / 3600.0
            for a, b in zip(runs, runs[1:])
        ]

        if not gaps:
            print(
                f"{CADENCE_SOURCE}: {len(runs)} run(s) recorded, so there is no "
                f"gap to measure — using {FALLBACK_MAX_AGE_HOURS:.0f}h until "
                f"there is. If history is disabled in the chart "
                f"(history.enabled=false) this is permanent rather than "
                f"temporary, and the number is a guess covering a nightly "
                f"schedule; set it deliberately or turn history on."
            )
            return FALLBACK_MAX_AGE_HOURS

        # Median once there are enough gaps for a middle value to mean
        # something; the largest gap while there are not. With one or two
        # samples the pessimistic reading is the honest one — a median of two
        # numbers is their average, which on a nightly install that has only
        # just started would sit below a normal cycle and fire on a healthy
        # job. Erring long here costs a little detection latency for a day or
        # two; erring short costs the monitor's credibility permanently.
        if len(gaps) >= ROBUST_GAPS:
            basis, how = statistics.median(gaps), "median"
        else:
            basis, how = max(gaps), "largest of only %d" % len(gaps)

        # Never below what the most recent gap implies, which is what makes a
        # changed schedule safe. An install that ran hourly for a week and then
        # moved to nightly has a median still dominated by the old cadence, so
        # a pure median would hold the threshold at 3h and go red the very day
        # the schedule changed — punishing a deliberate, correct action. The
        # latest gap is the pipeline's most current statement about itself.
        # It costs one cycle of extra tolerance after a genuinely slow run,
        # which is the cheaper mistake.
        if gaps[-1] > basis:
            basis, how = gaps[-1], f"{how} {basis:.2f}h overridden by latest"

        threshold = min(2.0 * basis + 1.0, CEILING_HOURS)

        print(
            f"{CADENCE_SOURCE}: {len(runs)} runs, {how} gap {basis:.2f}h "
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
