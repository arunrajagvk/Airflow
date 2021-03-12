from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
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
        hive_cli_conn_id="hive_conn",
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

    ## HIVE OPERATOR
    # /Library/Frameworks/Python.framework/Versions/3.8/bin/python3 -m pip install --upgrade pip
    # python3 -m pip install apache-airflow-providers-apache-hive
    ## template_fields = ['hql', 'schema', 'hive_cli_conn_id', 
    # 'mapred_queue', 'hiveconfs', 'mapred_job_name', 'mapred_queue_priority']
    # Let's say this is your kerberos ticket (likely from a keytab used for the remote service):

    # Ticket cache: FILE:/tmp/airflow_krb5_ccache
    # Default principal: hive/myserver.myrealm@myrealm

    # Valid starting       Expires              Service principal
    # 06/14/2018 17:52:05  06/15/2018 17:49:35  krbtgt/myrealm@myrealm
    # 	renew until 06/17/2018 05:49:33

    # # Here's what creating a connection looks like:

    #  airflow connections --add \
    # --conn_id metastore_cluster1 \
    # --conn_type 'hive_metastore' \
    # --conn_host 'myserver.mydomain \
    # --conn_port 9083 \
    # --conn_extra '{"authMechanism":"GSSAPI", "kerberos_service_name":"hive"}'

    # # But this won't work unless you make sure your airflow.cfg has this:
    # security = kerberos

    # # Note: I'm not sure what else that "security = kerberos" setting really does.
    # # I had it unset for quite a while and was still able to integrate with Kerberized Hive just fine.

    # # Test it from a CLI:
    # (venv) [airflow@mymachine dags]$ python
    # >>> from airflow.hooks import hive_hooks
    # >>> hm = hive_hooks.HiveMetastoreHook(metastore_conn_id='metastore_cluster1')
    # [2018-06-14 18:07:34,736] {base_hook.py:80} INFO - Using connection to: myserver.mydomain
    # >>> hm.get_databases()

    # file_name = "to_" + channel + "_" + yesterday.strftime("%Y-%m-%d") + ".csv"

    #     load_to_hdfs = BashOperator(
    #         task_id="put_" + channel + "_to_hdfs",
    #         bash_command="HADOOP_USER_NAME=hdfs hadoop fs -put -f "
    #         + local_dir
    #         + file_name
    #         + hdfs_dir
    #         + channel
    #         + "/",
    #     )

    #     load_to_hdfs << analyze_tweets

    #     load_to_hive = HiveOperator(
    #         task_id="load_" + channel + "_to_hive",
    #         hql="LOAD DATA INPATH '" + hdfs_dir + channel + "/" + file_name + "' "
    #         "INTO TABLE " + channel + " "
    #         "PARTITION(dt='" + dt + "')",
    #     )