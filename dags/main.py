from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import dag, task
import requests
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime


BASE_URL = "https://ll.thespacedevs.com/2.0.0/launch/upcoming/"

# Dictionary holding records of all the rocket launches:
rocket_info = [] 





default_arguements = {
    "owner": "Nishan",
    "retries": 5,
    "retry_delay": timedelta(seconds=2)
}

# TODO:2 -> Instantiating DAG and creating our DAG:
@dag(dag_id="rocket_info_etl_pipeline", 
start_date=datetime(2023,1,1),schedule_interval="@daily", 
catchup=True, default_args=default_arguements)
def rocket_info_etl():



    # TASK:1 Extract the data from the API:
    @task()
    def extract_data():
        response = requests.get(BASE_URL)
        response = response.json()
        return response
    



    # TASK:2 Recieve the JSON and transform it:
    @task()
    def transform_data(data:dict):
        data = data['results']
        for i in range(0, len(data)):
            rocket_info.append(
                {
                    "id": data[i]['id'],
                    "name": data[i]['name'],
                    "launch_status": data[i]['status']['name'],
                    "window_start": data[i]['window_start'],
                    "window_end": data[i]['window_end'],
                    "launcher_name": data[i]['launch_service_provider']['name'],
                    "launcher_type": data[i]['launch_service_provider']['type'],
                    # "rocket_description": data[i]['mission'].get('description'),
                    "total_launch_count": int(data[i]['pad']['location']['total_launch_count']),
                    "total_landing_count": data[i]['pad']['location']['total_landing_count'],
                    "image_url": data[i]['image']
                }
                )

        return rocket_info
            

        
    # TASK:3 -> Connect to postgres DB, create a table and load the data into it:
    @task()
    def load_data(info):
        
        # Instantiating the postgres hook:
        pg_hook = PostgresHook(postgres_conn_id="postgres_conn")

        sql = """
            CREATE TABLE IF NOT EXISTS rocket_info_table(
                id VARCHAR PRIMARY KEY,
                name VARCHAR(255),
                launch_status VARCHAR(255),
                window_start VARCHAR(255),
                window_end VARCHAR(255),
                launcher_name VARCHAR(255),
                launcher_type VARCHAR(255),
                total_launch_count INTEGER,
                total_landing_count INTEGER,
                image_url VARCHAR(255)
            )
        """

        # Execute the SQL command to create the table:
        pg_hook.run(sql)
        print("Table creation success!!")
       


        # Insert the data into the table:
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        columns = ', '.join(info[0].keys())
        values = []
        for i in range(0, len(info)):
            values.append(', '.join(f"'{val}'" for val in info[i].values()))
      
        for  val in values:
            sql = f"INSERT INTO rocket_info_table ({columns}) VALUES ({val});"
            cursor.execute(sql)
        conn.commit()

        print("Data Loaded Successfully!!")


    # Defining the structure of tasks:
    response_dict = extract_data()
    info = transform_data(response_dict)
    load_data(info)


etl_pipeline = rocket_info_etl()
