"""
-- Airflow

Airflow is an orchestrator, not a processing framework, process your gigabytes of data outside of Airflow (i.e. You have a Spark cluster, you use an operator to execute a Spark job, the data is processed in Spark).

// Airflow is an orchestrator allowing you to execute your tasks in the right time, right way at the right order.
Benefits:
-- Dynamic - Python 
-- Highly scalable - using Kubernetes too 
-- UI - Monitor DP, interact tasks 
-- Extensible - No need to wait for inbuilt plugin, we can build/ customize our instance 

Core components:

-- Web Server : Flask Server 
-- Scheduler: Deamon incharge of scheduling DP (Heart of Airflow)
-- Metastore: DB usually Postgres, responsible to store metadata, tasks and so on 
-- Executor: How tasks are executed - Using Kubernetes tasks, executes 
-- Worker: The process/sub-process executes the tasks 

An Executor defines how your tasks are execute whereas a worker is a process executing your task

The scheduler schedules your tasks, the web server serves the UI, the database stores the metadata of Airflow.


Core Component:

DAG:

A DAG is a data pipeline

1->2->3 (flow should not be looped)


OPERATOR: - an Operator is a task.

A wrapper around the class to do operations, connecting a database will require an operator that passes data. 
3 types:
-- Action Operator: Executing - Functions/commands (Bash Operator/ Python Operator)
-- Transfer Operator: Allow to trasfer data from S->D Presto data to Mysql 
-- Sensor Operator: Wait for something to perform -> Example: File sensor waits for some files to be in the folder to perform   

Task/Task Instance:
-- As soon as a task is triggered in DP, that task becomes a task instance.
-- A executor also becomes task instance

WORKFLOW:

All put together is called a workflow

## ARCHITECTURE

Single Node: 
webserver // Scheduler // Metastore // Executor

airflow db init is the first command to execute to initialise Airflow

If a task fails, check the logs by clicking on the task from the UI and "Logs"

The Gantt view is super useful to sport bottlenecks and tasks are too long to execute


Section 1:

Airflow - Hands On

open Aiflow.ova 
1. Visual Studio Code 
2. Install all extensions - remote-ssh, git pull, docker, yaml 
3. Windows F1 - Type "remote-ssh: Connect to host" then "Add host"
4. Type - "ssh -p 2222 airflow@localhost"
5. choose the .config file 
6. Check the added config file in the VS code 
7. Again press F1 and choose remote-ssh: Connect to host "localhost" 
8. Might promt for "linux", then "yes" to add key 
9. Then password : "airflow" 
10. Terminal menu click open "New terminal" 
airflow@airflowvm:~$ python3 -m venv sandbox
airflow@airflowvm:~$ source sandbox/bin/activate
(sandbox) airflow@airflowvm:~$ pip install wheel
(sandbox) airflow@airflowvm:~$ pip install apache-airflow==2.0.0 --constraint https://gist.githubusercontent.com/marclamberti/742efaef5b2d94f44666b0aec020be7c/raw/5da51f9fe99266562723fdfb3e11d3b6ac727711/constraint.txt 
$ airflow db init (to inititalize metadata/db settings)
$ airflow webserver
$ airflow users create -u admin -p admin -f arunraja -l gvk -r Admin -e arunrajagvk.edu@gmail.com

-- localhost:8080 / login : admin pwd: admin
$ airflow scheduler 

-- Followed by UI 

Creating First Data Pipeline 
Steps:

1. Creating table 
2. Is_api_available 
3. extracting_user from the api 
4. Processing_user 
5. Storing_user

DO's

One operator one task 

-- Provider packages : http://airflow.apache.org/docs/apache-airflow-providers/packages-ref.html

-- Do pip install 

-- Add connection details to the Admin -- Operators 

Step1: ConnID: 'db_sqlite' (same as in the dag file)
Step2: Connection type: sqlite
Step3: Host: /home/airflow/airflow/airflow.db


Testing a Task:

$ airfow tasks test <dag_name> <task_name> <exec date>
airflow tasks test user_processing creating_table 2020-01-01

airflow tasks test user_processing is_api_available 2020-01-01


-- Scheduling a dag IMPORTANT:

the catchup=True will trigger all the non executed dags starting from the recent execution_date 
and not the start date mentioned in the dag

If catchup=False it will only trigger for the current execution date 

ALL dates are UTC 

we can change the timezone from >> airflow.cfg file "default_ui_timezone=utc"


"""
with DAG('user_processing', schedule_interval='@daily', 
        default_args=default_args, catchup=False) as dag:

"""
Default Executor for Airflow: SequentialExecutor
airflow@airflowvm:~/airflow$ airflow config get-value core sql_alchemy_conn
sqlite:////home/airflow/airflow/airflow.db
airflow@airflowvm:~/airflow$ ls
airflow-webserver.pid  airflow.cfg  airflow.db  dags  logs  unittests.cfg  webserver_config.py

--Sqlite doesnt allow multiple writes at the same time that's why we cannot use sql lite for parallel

airflow@airflowvm:~/airflow$ airflow config get-value core executor
SequentialExecutor


-- To execute multiple tasks parallel, we have to change the DB to Postgres which supports multiple read and write parallel

-- Change the executor to LocalExecutor - converts tasks to subprocess and runs parallel in machine 

Step1:

airflow@airflowvm:~/airflow$ sudo apt update 
airflow@airflowvm:~/airflow$ sudo apt install postgresql

-- open postgresql 

(sandbox) airflow@airflowvm:~$ sudo -u postgres psql

$$ ALTER TABLE user PASSWORD 'postgres';
\q

Step 2:
-- Change airflow.cfg

sql_alchemy_conn = postgresql+psycopg2://postgres:postgres@localhost/postgres

# The executor class that airflow should use. Choices include
# ``SequentialExecutor``, ``LocalExecutor``, ``CeleryExecutor``, ``DaskExecutor``,
# ``KubernetesExecutor``, ``CeleryKubernetesExecutor`` or the
# full import path to the class when using a custom executor.

executor = LocalExecutor


Step 3: 

-- Stop airflow scheduler and webserver 
-- (sandbox) airflow@airflowvm:~/airflow$ airflow db init 

-- Create user :

(sandbox) airflow@airflowvm:~/airflow$ airflow users create -u admin -p admin -r Admin -f admin -ladmin -e admin@airflow.com


"""



"""
Scale executors - Multiple machines/nodes 

to run multiple tasks parallelly in cluster : CeleryExecutor/KubernetesExecutor play major role.


"""



