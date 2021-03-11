from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime 

default_args = {
    'start_date': datetime(2020, 1, 1)
}

with DAG('trigger_rule',default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    
    task1 = BashOperator(
        task_id='task1',
        bash_command='exit 1',
        do_xcom_push=False
    )

    task2 = BashOperator(
        task_id='task2',
        bash_command='exit 1',
        do_xcom_push=False
    )

    task3 = BashOperator(
        task_id='task3',
        bash_command='exit 0',
        do_xcom_push=False,
        trigger_rule='all_failed'
    )

[task1, task2] >> task3