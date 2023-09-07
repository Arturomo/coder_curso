import sys, os, pendulum
# sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from download_info_api import create_tables, insert_weather_info, chek_seismological, send_email

DAG_ID = 'Api_weather'
DAG_DESCRIPTION = 'Extrae información climatica y sismica de los estados de México'
DAG_SCHEDULE = '30 5 */1 * *'
DAG_CATCHUP = False
TAGS = ["Proyecto_fianl"]

ARGS = {
    'owner' : 'Arturo',
    'start_date' : pendulum.datetime(2022, 1, 1, tz="America/Mexico_City")
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
    
    send_alert = BranchPythonOperator(task_id="Alert",
                                            python_callable=chek_seismological,
                                            do_xcom_push=False,
                                            provide_context=True)
    
    Send_alert = PythonOperator(task_id = 'Send_alert',
                                        python_callable = send_email,
                                        retries = 1,
                                        retry_delay = timedelta(minutes=1))
    
    no_alert = EmptyOperator(task_id="No_alert")
    
    create_table_if_exist_task >> insert_weather_info_task >> send_alert >> [Send_alert, no_alert]
