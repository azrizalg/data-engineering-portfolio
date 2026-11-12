import pendulum

from airflow.sdk import dag, task


@dag(
    dag_id="get_connote_test",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["learning", "etl"],
)
def get_connote_test():

    @task(retries=2, retry_delay=pendulum.duration(seconds=10))
    def get_data():
        
        import requests 
        url = "https://api-raise-pro.jne.co.id/230/utility/get-connote/awb-date?awb_date_from=1789405200000&awb_date_to=1789408800000&page=1&limit=10"
        response = requests.get(url)

        response.raise_for_status()
        # Check if the request succeeded (Status Code 200)
        if response.status_code == 200:
            data = response.json()  # Parse JSON response into a Python dictionary
            print(data)
        else:
            print(f"Error: {response.status_code}")

        return data


    extracted_data = get_data()


get_connote_test()