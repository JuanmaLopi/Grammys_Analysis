import sys
import os

# Agregar la ruta al paquete src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Importar funciones desde los módulos en src
from extract import read_db
from db_cleaning import transform_db
from csv_cleaning import read_csv, transform_csv
from merge import merge
from upload_to_postgres import upload_to_postgres
from upload_to_drive import upload_to_google_drive

# Definir el DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'grammys_etl_pipeline',
    default_args=default_args,
    description='ETL Pipeline for Grammys Analysis',
    schedule_interval='@daily',
) as dag:

    # Definir las tareas
    
    read_db_task = PythonOperator(
        task_id='read_db',
        python_callable=read_db,
        dag=dag,
    )

    transform_db_task = PythonOperator(
        task_id='transform_db',
        python_callable=transform_db,
        dag=dag,
    )

    read_csv_task = PythonOperator(
        task_id='read_csv',
        python_callable=read_csv,
        dag=dag,
    )

    transform_csv_task = PythonOperator(
        task_id='transform_csv',
        python_callable=transform_csv,
        dag=dag,
    )

    merge_task = PythonOperator(
        task_id='merge',
        python_callable=merge,
        dag=dag,
    )

    upload_to_postgres_task = PythonOperator(
        task_id='upload_to_postgres',
        python_callable=upload_to_postgres,
        dag=dag,
    )

    upload_to_google_drive_task = PythonOperator(
        task_id='upload_to_google_drive',
        python_callable=upload_to_google_drive,
        dag=dag,
    )

    # Definir la secuencia de tareas
    read_db_task >> transform_db_task >> merge_task
    read_csv_task >> transform_csv_task >> merge_task >> upload_to_postgres_task >> upload_to_google_drive_task