import pendulum

from airflow.sdk import dag, task


@dag(
    dag_id="customer_etl_demo",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["learning", "etl"],
)
def customer_etl_demo():

    @task(retries=2, retry_delay=pendulum.duration(seconds=10))
    def extract():
        
        import random 
        if random.random() < 0.7: raise ValueError("Simulated temporary extraction failure")
        
        customers = [
            {"id": 1, "name": "Alice", "city": "Jakarta"},
            {"id": 2, "name": "Bob", "city": "Bogor"},
            {"id": 3, "name": "Charlie", "city": "jakarta"},
        ]

        print(f"Extracted {len(customers)} customers")

        return customers

    @task
    def transform(customers):
        for customer in customers:
            customer["city"] = customer["city"].title()

        print("Customer data transformed")

        return customers

    @task
    def load(customers):
        print(f"Loading {len(customers)} customers")

        for customer in customers:
            print(customer)

    extracted_data = extract()
    transformed_data = transform(extracted_data)
    load(transformed_data)


customer_etl_demo()