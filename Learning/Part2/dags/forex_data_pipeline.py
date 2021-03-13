from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_jdbc import SparkJDBCOperator
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.email import EmailOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

from datetime import datetime, timedelta
import json
import csv 
import requests

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 3, 1),
    'depents_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'email': 'arunrajagvk@gmail.com',
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
}

def _download_rates():
    with open('/Users/arunraja/airflow/dags/files/forex_currencies.csv') as forex_currencies:
        reader = csv.DictReader(forex_currencies, delimiter=';')
        for row in reader:
            base = row['base']
            with_pairs = row['with_pairs'].split(' ')
            indata = requests.get('https://api.exchangeratesapi.io/latest?base=' + base).json()
            outdata = {'base': base, 'rates': {}, 'last_update': indata['date']}
            for pair in with_pairs:
                outdata['rates'][pair] = indata['rates'][pair]
            with open('/Users/arunraja/airflow/dags/files/forex_rates.json', 'a') as outfile:
                json.dump(outdata, outfile)
                outfile.write('\n')

# SLACK_CONN_ID = 'slack'
# def task_fail_slack_alert(context):
#     slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
#     slack_msg = """
#             :red_circle: Task Failed. 
#             *Task*: {task}  
#             *Dag*: {dag} 
#             *Execution Time*: {exec_date}  
#             *Log Url*: {log_url} 
#             """.format(
#             task=context.get('task_instance').task_id,
#             dag=context.get('task_instance').dag_id,
#             ti=context.get('task_instance'),
#             exec_date=context.get('execution_date'),
#             log_url=context.get('task_instance').log_url,
#         )
#     failed_alert = SlackWebhookOperator(
#         task_id='slack_test',
#         http_conn_id='slack',
#         webhook_token=slack_webhook_token,
#         message=slack_msg,
#         username='airflow')
#     return failed_alert.execute(context=context)


# task_with_failed_slack_alerts = BashOperator(
#     task_id='fail_task',
#     bash_command='exit 1',
#     on_failure_callback=slack_failed_task,
#     provide_context=True
# )

with DAG(dag_id='forex_data_pipeline',
        default_args=default_args, 
        schedule_interval='@daily',
        catchup=False) as dag:

    is_forex_rates_available = HttpSensor(
        task_id='is_forex_rates_available',
        method='GET',
        http_conn_id='forex_api',
        endpoint='latest',
        response_check=lambda response: 'rates' in response.text,
        poke_interval=5,
        timeout=20
    )

    is_forex_currencies_file_available = FileSensor(
        task_id='is_forex_currencies_file_available',
        fs_conn_id='forex_path',
        filepath='forex_currencies.csv',
        poke_interval=5,
        timeout=20
    )

    downloading_rates = PythonOperator(
        task_id='downloading_rates',
        python_callable=_download_rates
    )

    saving_rates = BashOperator(
        task_id='saving_rates',
        bash_command="""
            hdfs dfs -mkdir -p /forex && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/forex_rates.json /forex
        """
    )

    creating_forex_rates_table = HiveOperator(
        task_id="creating_forex_rates_table",
        hive_cli_conn_id="hive_default",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS forex_rates(
                base STRING,
                last_update DATE,
                eur DOUBLE,
                usd DOUBLE,
                nzd DOUBLE,
                gbp DOUBLE,
                jpy DOUBLE,
                cad DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """
    )

    forex_processing = SparkSubmitOperator(
        task_id='forex_processing',
        conn_id='spark_conn',
        application="/User/arunraja/airflow/dags/scripts/forex_processing.py",
        verbose=False,
        executor_cores=2,
        num_executors=2,
        executor_memory='256M',
        driver_memory='1G'
    )

    sending_email_notification = EmailOperator(
            task_id="sending_email",
            to="airflow_course@yopmail.com",
            subject="forex_data_pipeline",
            html_content="""
                <h3>forex_data_pipeline succeeded</h3>
            """
        )
    

    sending_slack_notification = SlackAPIPostOperator(
        task_id="sending_slack",
        token='asdljfwiei3234kndoiff2309e',
        username="airflow",
        text="DAG forex_data_pipeline: DONE",
        channel="#airflow-exploit"
    )

    is_forex_rates_available >> is_forex_currencies_file_available >> downloading_rates >> saving_rates >> creating_forex_rates_table >> forex_processing 
    forex_processing >> sending_email_notification >> sending_slack_notification