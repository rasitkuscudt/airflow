from airflow.sdk import dag, task
import pendulum


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["smoke"],
)
def smoke_test():
    @task
    def hello():
        print("dags are syncing")

    hello()


smoke_test()
