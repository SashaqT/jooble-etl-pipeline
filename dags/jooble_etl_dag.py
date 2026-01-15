from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow/scripts')

from extract import extract_jooble_data
from transform import transform_data
from load import load_and_validate

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='jooble_data_engineer_etl',
    default_args=default_args,
    description='ETL pipeline for Data Engineer vacancies from Jooble',
    schedule_interval='0 */6 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'jooble', 'data-engineer'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract_jooble_data',
        python_callable=extract_jooble_data,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id='load_and_validate',
        python_callable=load_and_validate,
    )

    extract_task >> transform_task >> load_task