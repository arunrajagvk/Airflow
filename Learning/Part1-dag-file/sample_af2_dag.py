from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.models.baseoperator import chain, cross_downstream 

from datetime import datetime, timedelta

default_args={
    'retries': 5
    'retry_delay': timedelta(minutes=5) 
}

def _downloading_data(**kwargs):
    with open('/tmp/my_files.txt', 'w') as f:
        f.write('my_data')

def _checking_data():
    print("checking data")

with DAG(dag_id='sample_af2_dag', default_args=default_args, 
    schedule_interval='@daily', catchup=False, start_date=datetime(2021, 1, 1)) as dag:

    downloading_data = PythonOperator(
        task_id='downloading_data',
        python_callable=_downloading_data
    )

    checking_data = PythonOperator(
        task_id='downloading_data',
        python_callable=_checking_data
    )

    waiting_for_data = FileSensor(
        task_id='waiting_for_data',
        fs_conn_id='fs_default',
        file_path='my_files.txt'
    )

    processing_data = BashOperator(
        task_id='processing_data',
        bash_command='exit 0'
    )

cross_downstream([downloading_data, checking_data], [waiting_for_data,processing_data])