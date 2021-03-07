from airflow import DAG
from datetime import datetime 
from airflow.operators.bash import BashOperator

default_args = {
    'start_date': datetime(2020, 1, 1)
}

with DAG ('parallel_dag', schedule_interval='@daily', 
        default_args=default_args, catchup=False) as dag:
    
    task1 = BashOperator(
        task_id='task1',
        bash_command='sleep 3'
    
    )

    task2 = BashOperator(
        task_id='task2',
        bash_command='sleep 3'
    
    )

    task3 = BashOperator(
        task_id='task3',
        bash_command='sleep 3'
    
    )

    task4 = BashOperator(
        task_id='task4',
        bash_command='sleep 3'
    
    )

task1 >> [task2, task3] >> task4
