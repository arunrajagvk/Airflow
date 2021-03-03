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

Core Component:

A DAG is a data pipeline, an Operator is a task.

An Executor defines how your tasks are execute whereas a worker is a process executing your task

The scheduler schedules your tasks, the web server serves the UI, the database stores the metadata of Airflow.

airflow db init is the first command to execute to initialise Airflow

If a task fails, check the logs by clicking on the task from the UI and "Logs"

The Gantt view is super useful to sport bottlenecks and tasks are too long to execute

"""



