"""Ask whether the grid-loss results are plausible, not just present.

WHY THIS IS SEPARATE FROM grid_ml_output_check. That one asks "did the job
write?" — it reads object timestamps and nothing else. This asks "is what it
wrote believable?", which is the wider blind spot: a pipeline producing
confidently wrong numbers looks healthier than one producing nothing at all,
because every freshness check passes.

We have already been on the wrong side of this. The generator ran for days
with the transformer feed publishing at half its intended rate, so the energy
balance came out negative — honest zones appeared to deliver less than their
meters reported, which is physically impossible. Nothing flagged it. It was
noticed because somebody happened to look at a dashboard.

Each check below is a statement about the world that the data should not be
able to contradict. They are deliberately loose: the point is to catch a
pipeline that has broken, not to police the model's judgement. A check that
fires on a normal day gets muted within a week and then protects nothing.
"""

from __future__ import annotations

import pendulum
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.sdk import dag, task

CONN = "trino"

# Fully qualified on purpose. The Trino provider has disagreed with itself
# across versions about whether a connection's `schema` means catalog or
# schema; naming both makes the question moot.
LOSSES = "hive.grid.grid_losses"
ANOMALIES = "hive.grid.meter_anomalies"
ZONES = "hive.grid.zone_summary"
ZONES_HISTORY = "hive.grid.zone_summary_history"


def _one(sql: str):
    return TrinoHook(trino_conn_id=CONN).get_first(sql)


@dag(
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["grid", "monitoring"],
)
def grid_ml_result_checks():
    @task
    def physics() -> None:
        """Transformer input cannot be less than what its meters reported.

        A transformer delivers energy; its meters measure part of what it
        delivered. Metered above delivered is not a small error, it means the
        two sides of the comparison are not the same period — a feed running
        at the wrong rate, or a window that caught meters without their
        transformer. This is the check that would have caught the generator
        fault, and it is the only one here that is a law rather than a
        heuristic.

        A hair of tolerance because the hourly windows are assembled
        independently on each side and the last one is always partial.
        """
        row = _one(f"""
            SELECT count(*), min(loss_pct)
            FROM {LOSSES}
            WHERE loss_pct < -5
        """)
        bad, worst = row[0], row[1]
        print(f"zones with loss below -5%: {bad} (worst {worst})")
        if bad:
            raise ValueError(
                f"{bad} transformer-hours report more metered energy than was "
                f"delivered (worst {worst:.1f}%). That is not a modelling "
                f"error — the two sides of the balance are not covering the "
                f"same period. Check the transformer feed's publish rate "
                f"against the meters'."
            )

    @task
    def shape() -> None:
        """The result set should have the shape the grid has.

        Ten transformers and two hundred meters is the model this showcase
        generates. A count that has collapsed means the lookback window caught
        almost nothing; a count that has grown means something else is writing
        into the same tables. Neither is a judgement about theft.
        """
        zones = _one(f"SELECT count(*) FROM {ZONES}")[0]
        meters = _one(f"SELECT count(*) FROM {ANOMALIES}")[0]
        print(f"zones={zones} meters={meters}")

        problems = []
        if not 5 <= zones <= 50:
            problems.append(f"{zones} zones (expected around 10)")
        if not 50 <= meters <= 1000:
            problems.append(f"{meters} meters (expected around 200)")
        if problems:
            raise ValueError(
                "the result set is not the shape the grid is: "
                + "; ".join(problems)
                + ". Either the analysis window caught very little, or "
                "something else is writing to these tables."
            )

    @task
    def stability() -> None:
        """The answer should not lurch between runs.

        Theft does not start and stop hourly. A suspect count that jumps from
        two to nine between consecutive runs is a statement about the pipeline,
        not about the network — most often a partial window, occasionally a
        threshold that moved because the input distribution did.

        Needs the history tables, and says so rather than passing quietly when
        they are missing: a check that silently does nothing is worse than one
        that is absent, because it looks like coverage.
        """
        try:
            rows = TrinoHook(trino_conn_id=CONN).get_records(f"""
                SELECT run_ts, count(*) FILTER (WHERE zone_suspect) AS suspect
                FROM {ZONES_HISTORY}
                GROUP BY run_ts ORDER BY run_ts DESC LIMIT 2
            """)
        except Exception as e:  # noqa: BLE001
            raise ValueError(
                f"{ZONES_HISTORY} could not be read, so run-to-run stability "
                f"cannot be checked. Register the history tables — section 5.1 "
                f"of the Grid Loss showcase — or remove this task. Underlying "
                f"error: {e}"
            ) from e

        if len(rows) < 2:
            print("only one run recorded so far; nothing to compare yet")
            return

        (new_ts, new_n), (old_ts, old_n) = rows[0], rows[1]
        print(f"{old_ts}: {old_n} suspect  →  {new_ts}: {new_n} suspect")
        if abs(new_n - old_n) > 3:
            raise ValueError(
                f"suspect zones moved from {old_n} to {new_n} between "
                f"{old_ts} and {new_ts}. Theft does not appear and disappear "
                f"within an hour, so suspect the window: check the record "
                f"counts in the driver log for the newer run."
            )

    physics()
    shape()
    stability()


grid_ml_result_checks()
