"""Promote the grid-loss history into a maintained Iceberg table.

WHY THIS EXISTS. The ML job writes plain Parquet and the showcase explains
why: HMS 4.x dropped the legacy thrift API that Spark 3.5's bundled Hive
2.3.9 client speaks, so Spark cannot reach the metastore at all here. That
decision is unchanged by this DAG — nothing about the Spark job moves. What
changes is what happens to the history *after* it lands.

The history tables are append-only Parquet directories, and the showcase is
candid about the compromise: partitioning was rejected because a partitioned
Hive external table needs every new partition registered in the metastore,
which would mean calling sync_partition_metadata from somewhere on a
schedule — "another thing that fails without saying so". Appending avoided
that and bought two problems instead:

  1. One file per run per table. Hourly, that is 8760 files a year in each
     location, and a Hive external table has no compaction path. Nothing in
     the stack can merge them.
  2. No retention. The locations grow forever, with no way to drop old rows
     short of rewriting the directory by hand.

Iceberg answers both, and Trino can write it even though Spark cannot —
Trino's client is HMS-4 compatible, which is exactly why the one-time DDL in
section 5 of the showcase is a Trino job and not a Spark one. So this DAG is
a Trino DAG. It needs nothing new from the chart: no RBAC, no ServiceAccount,
no Kubernetes API. Just AIRFLOW_CONN_TRINO, which the platform already
generates, and the bucket name, which it already passes.

WHAT IT DOES, hourly:

  hive.grid.*_history          append-only Parquet, one small file per run
        │                      (unchanged — still the Spark job's output)
        │  incremental, by run_ts watermark
        ▼
  iceberg.grid_history.*       partitioned by month(run_ts), compacted,
                               snapshots expired after 7 days
                               (its own metastore database — see below)

Each step is idempotent. The load is bounded by the watermark already in the
Iceberg table, so a repeated or missed run duplicates nothing and needs no
catchup.

The Iceberg tables are MANAGED, not external: no external_location, so
Iceberg owns the files and DROP TABLE deletes them. The opposite of the Hive
tables in section 5, where DROP leaves the Parquet where Spark put it. Worth
knowing before dropping anything.

Scheduled at :30 on purpose. The Spark job starts at :00, and while its
appends are individually atomic — a new file either appears or does not —
there is no reason to read a location in the minute something is writing to
it, and half an hour is past even a slow run.
"""

from __future__ import annotations

import os

import pendulum
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.sdk import dag, task

CONN = "trino"
BUCKET = os.environ.get("AIRFLOW_VAR_S3_BUCKET", "lakehouse")

# THE ICEBERG TABLES GET THEIR OWN METASTORE DATABASE, and the first version
# of this did not, which is worth writing down because the reasoning behind
# the mistake is so plausible.
#
# Two Trino catalogs over the same metastore are not two namespaces. Both the
# hive and iceberg catalogs in this platform point at the same HMS — the same
# `<release>-hive` ConfigMap — so `hive.grid` and `iceberg.grid` are the SAME
# database. The catalog prefix names a connector, not a container.
#
# So CREATE TABLE iceberg.grid.grid_losses_history found the Hive external
# table that section 5.1 registered, saw that it was not an Iceberg table, and
# refused: "Table 'iceberg.grid.grid_losses_history' of unsupported type
# already exists". Not a permissions problem, not a connector problem — the
# name was simply taken.
#
# Hence a separate database. The `_history` suffix then drops off the table
# names: every table in this schema is history, so repeating it would be
# noise. hive.grid.zone_summary_history becomes iceberg.grid_history.zone_summary.
ICEBERG_SCHEMA = "grid_history"

# s3a:// because that is the scheme the showcase's Hive DDL uses and is
# therefore proven on this cluster. Trino's native S3 filesystem prefers
# s3://, and the Iceberg connector may not accept the alias on every
# version. If `ensure` fails on the CREATE SCHEMA with a filesystem or URI
# error, this constant is the one-character fix.
SCHEME = "s3a"
WAREHOUSE = f"{SCHEME}://{BUCKET}/iceberg/grid"

# Column lists are the single source of truth: both the CREATE TABLE and the
# INSERT's column list are generated from them, so the two cannot drift into
# a positional mismatch. They mirror the Hive DDL in section 5.1 of the
# showcase exactly — if that changes, this must change with it, and the
# `columns_match` check below is what notices.
TABLES: dict[str, list[tuple[str, str]]] = {
    "grid_losses_history": [
        ("transformer_id", "varchar"),
        ("window_start", "timestamp"),
        ("window_end", "timestamp"),
        ("input_kwh", "double"),
        ("metered_kwh", "double"),
        ("loss_kwh", "double"),
        ("loss_pct", "double"),
        ("suspect", "boolean"),
        ("active_meters", "bigint"),
        ("run_ts", "timestamp"),
    ],
    "meter_anomalies_history": [
        ("meter_id", "varchar"),
        ("transformer_id", "varchar"),
        ("reading_cnt", "bigint"),
        ("mean_kwh", "double"),
        ("std_kwh", "double"),
        ("night_ratio", "double"),
        ("zero_ratio", "double"),
        ("cluster", "integer"),
        ("anomaly_score", "double"),
        ("profile_anomaly", "boolean"),
        ("zone_loss_pct", "double"),
        ("zone_suspect", "boolean"),
        ("risk_level", "varchar"),
        ("run_ts", "timestamp"),
    ],
    "zone_summary_history": [
        ("transformer_id", "varchar"),
        ("input_kwh", "double"),
        ("metered_kwh", "double"),
        ("loss_pct", "double"),
        ("zone_suspect", "boolean"),
        ("anomalous_meters", "bigint"),
        ("suspect_type", "varchar"),
        ("run_ts", "timestamp"),
    ],
}


def _hook() -> TrinoHook:
    return TrinoHook(trino_conn_id=CONN)


def _run(sql: str) -> None:
    _hook().run(sql)


def _one(sql: str):
    return _hook().get_first(sql)


def _cols(table: str) -> str:
    return ", ".join(name for name, _ in TABLES[table])


def _target(table: str) -> str:
    """Iceberg name for a Hive history table.

    Keyed off the Hive name everywhere else, so the two never need to be kept
    in step by hand: hive.grid.zone_summary_history -> the schema above,
    without the suffix the schema already says.
    """
    return f"iceberg.{ICEBERG_SCHEMA}.{table.removesuffix('_history')}"


def _meta(table: str, kind: str) -> str:
    """One of Iceberg's metadata tables — $files, $snapshots, $partitions.

    The `$` has to be inside the quotes: the schema is an identifier, the
    table-plus-suffix is a single quoted one.
    """
    return f'iceberg.{ICEBERG_SCHEMA}."{table.removesuffix("_history")}${kind}"'


@dag(
    schedule="30 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["grid", "iceberg"],
)
def grid_iceberg_history():
    @task
    def ensure() -> list[str]:
        """Create the schema and the three managed Iceberg tables, idempotently.

        The partitioning is the whole point: `month(run_ts)` is Iceberg's
        hidden partitioning, which needs no metastore registration for new
        partitions and no sync_partition_metadata call. That single line is
        what the showcase had to work around, and the reason the history is
        appended rather than partitioned today.
        """
        _run(
            f"CREATE SCHEMA IF NOT EXISTS iceberg.{ICEBERG_SCHEMA} "
            f"WITH (location = '{WAREHOUSE}')"
        )

        for table, cols in TABLES.items():
            body = ",\n  ".join(f"{name} {typ}" for name, typ in cols)
            _run(f"""
                CREATE TABLE IF NOT EXISTS {_target(table)} (
                  {body}
                ) WITH (
                  partitioning = ARRAY['month(run_ts)'],
                  format = 'PARQUET'
                )
            """)
            print(f"{_target(table)} ready")

        return list(TABLES)

    @task
    def columns_match(table: str) -> str:
        """Refuse to load if the Hive table has drifted from what we declare.

        A silent positional mismatch is the failure this prevents: the INSERT
        names its columns, so a renamed or reordered Hive column would either
        error loudly (good) or, if two columns share a type, quietly load the
        wrong values into the wrong place (not good). Comparing the name sets
        catches the second case before any rows move.
        """
        rows = _hook().get_records(f"""
            SELECT column_name
            FROM hive.information_schema.columns
            WHERE table_schema = 'grid' AND table_name = '{table}'
        """)
        actual = {r[0] for r in rows}
        if not actual:
            raise ValueError(
                f"hive.grid.{table} is not registered. Run the one-time DDL in "
                f"section 5.1 of the Grid Loss showcase first — this DAG reads "
                f"those tables, it does not create them."
            )

        declared = {name for name, _ in TABLES[table]}
        if actual != declared:
            raise ValueError(
                f"hive.grid.{table} does not have the columns this DAG "
                f"declares. Only in Hive: {sorted(actual - declared)}. Only "
                f"declared here: {sorted(declared - actual)}. Update TABLES in "
                f"this file and the Iceberg table to match, then re-run."
            )
        return table

    @task
    def load(table: str) -> str:
        """Copy the runs that are not in the Iceberg table yet.

        The watermark comes back as the varchar Trino itself renders, and goes
        straight back into a TIMESTAMP literal. That round-trips exactly;
        formatting it from a Python datetime invites a precision mismatch
        between what we write and what Trino stored.
        """
        target = _target(table)
        watermark = _one(f"SELECT cast(max(run_ts) AS varchar) FROM {target}")[0]

        where = "" if watermark is None else f"WHERE run_ts > TIMESTAMP '{watermark}'"
        cols = _cols(table)
        _run(f"INSERT INTO {target} ({cols}) SELECT {cols} FROM hive.grid.{table} {where}")

        new_watermark, rows = _one(
            f"SELECT cast(max(run_ts) AS varchar), count(*) FROM {target}"
        )
        print(
            f"{table}: loaded up to {new_watermark} "
            f"(was {watermark or 'empty'}), {rows} rows total"
        )
        return table

    @task
    def maintain(table: str) -> str:
        """Compact the recent partitions and expire old snapshots.

        This is the task that does the thing a Hive external table cannot.
        The file count before and after is printed because it is the whole
        argument: hourly appends produce one small file each, and nothing in
        the Parquet-on-Hive setup can merge them.

        Optimize is limited to the last two days rather than the whole table.
        At this volume a full rewrite would be harmless, but rewriting every
        month of history every hour is the wrong shape and would quietly
        become expensive; the predicate prunes to the month partitions that
        actually changed. Some Trino versions reject a WHERE on a
        hidden-partition source column, so it falls back to a full optimize
        and says it did — a fallback that stays silent is how you end up
        believing you have a behaviour you do not.

        7 days of snapshots is the retention, which is also the time-travel
        window: FOR TIMESTAMP AS OF works within it and not before it.
        Trino's iceberg.expire-snapshots.min-retention defaults to 7 days, so
        this is the floor, not an arbitrary pick.
        """
        target = _target(table)
        files_sql = (
            f"SELECT count(*), coalesce(sum(file_size_in_bytes), 0) "
            f"FROM {_meta(table, 'files')}"
        )

        before_n, before_b = _one(files_sql)

        try:
            _run(
                f"ALTER TABLE {target} EXECUTE optimize "
                f"WHERE run_ts > current_timestamp - INTERVAL '2' DAY"
            )
        except Exception as e:  # noqa: BLE001
            print(f"partition-limited optimize refused ({e}); optimizing the whole table")
            _run(f"ALTER TABLE {target} EXECUTE optimize")

        after_n, after_b = _one(files_sql)
        _run(f"ALTER TABLE {target} EXECUTE expire_snapshots(retention_threshold => '7d')")

        print(
            f"{table}: {before_n} files ({before_b / 1024:.0f} KiB) -> "
            f"{after_n} files ({after_b / 1024:.0f} KiB)"
        )
        return table

    @task
    def report(tables: list[str]) -> None:
        """Print what Iceberg is now keeping, so the DAG shows its work.

        Not a check — nothing here fails. It exists so that one task's log
        answers "is Iceberg actually doing anything": how many snapshots are
        retained, how the months are partitioned, how many rows each holds.
        """
        for table in sorted(tables):
            snaps = _one(f"SELECT count(*) FROM {_meta(table, 'snapshots')}")[0]
            parts = _hook().get_records(f"""
                SELECT cast(partition AS varchar), record_count, file_count
                FROM {_meta(table, 'partitions')}
                ORDER BY 1
            """)
            print(f"\n{_target(table)}: {snaps} snapshots retained")
            for partition, records, files in parts:
                print(f"  {partition}  {records} rows in {files} file(s)")

    ready = ensure()
    checked = columns_match.expand(table=ready)
    loaded = load.expand(table=checked)
    maintained = maintain.expand(table=loaded)
    report(maintained)


grid_iceberg_history()
