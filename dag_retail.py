
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from etl_process import DataETLManager, DATA_PATH

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 3, 9),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

etl_dag = DAG(
    'etl_retail',
    default_args=default_dag_args,
    description='DAG for ETL retail process',
    schedule_interval=timedelta(days=1),
    tags=['retail']
)


etl_manager = DataETLManager(DATA_PATH, 'OnlineRetail.csv')

extract = PythonOperator(
    task_id='extract_data',
    python_callable=etl_manager.extract_data,
    dag=etl_dag
)

transform = PythonOperator(
    task_id='transform_data',
    python_callable=etl_manager.transform_data,
    dag=etl_dag
)

load = PythonOperator(
    task_id='load_data',
    python_callable=etl_manager.load_data,
    dag=etl_dag
)

extract >> transform >> load

