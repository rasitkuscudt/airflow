from airflow.sdk import Asset, dag, task
import pendulum

# The URI is just a label — Airflow never opens it. It is the join key
# between DAGs, so it must match the consumer's literal exactly.
METER_READINGS = Asset("s3://lakehouse/curated/meter_readings/")


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["assets", "demo"],
)
def asset_producer():
    @task(outlets=[METER_READINGS])
    def publish():
        print("wrote a new batch of meter readings")

    publish()


asset_producer()
