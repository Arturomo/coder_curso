import sys, os, pendulum
# sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from download_info_api import create_tables, insert_weather_info

DAG_ID = 'Api_weather'
DAG_DESCRIPTION = 'Api_weather'
DAG_SCHEDULE = '30 5 */1 * *'
DAG_CATCHUP = False
TAGS = ["Entregable_3"]

ARGS = {
    'owner' : 'Arturo',
    'start_date' : pendulum.datetime(2022, 1, 1, tz="America/Mexico_City"),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


with DAG(dag_id = DAG_ID,
         description = DAG_DESCRIPTION,
         default_args = ARGS,
         schedule_interval = DAG_SCHEDULE,
         catchup = DAG_CATCHUP,
         tags = TAGS,
         max_active_runs = 1) as dag:
    
    create_table_if_exist_task = PythonOperator(task_id = 'Create_table_if_exist',
                                         python_callable = create_tables,
                                         retries = 1,
                                         retry_delay = timedelta(minutes=1))
    
    insert_weather_info_task = PythonOperator(task_id = 'Insert_weather_info',
                                        python_callable = insert_weather_info,
                                        retries = 1,
                                        retry_delay = timedelta(minutes=1))
    
    create_table_if_exist_task >> insert_weather_info_task