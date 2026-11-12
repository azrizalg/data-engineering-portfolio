import pendulum

from airflow.sdk import dag, task


@dag(
    dag_id="hello_world",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["learning"],
)
def hello_world():

    @task
    def say_hello():
        print("Hello from Apache Airflow!")

    say_hello()


hello_world()
