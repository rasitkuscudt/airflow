from airflow.sdk import Asset, dag, task
import pendulum

# Same URI as the producer — this is what links the two.
METER_READINGS = Asset("s3://lakehouse/curated/meter_readings/")


@dag(
    schedule=[METER_READINGS],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["assets", "demo"],
)
def asset_consumer():
    @task
    def summarise():
        print("meter readings changed - recomputing the daily summary")

    summarise()


asset_consumer()
