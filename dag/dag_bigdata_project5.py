#!python airflow

from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from airflow.utils.dates import days_ago

default_args = {
    'owner': 'erale'}

dag = DAG(
    dag_id='bigdata_project5',
    start_date=days_ago(1),
    schedule_interval='*/8 * * * *',
    default_args=default_args)

t1 = DummyOperator(
    task_id='Start')

t2 = BashOperator(
    task_id='Dump Data',
    bash_command='python3 /home/erale/dags/script/dump.py',
    dag=dag)

t3 = BashOperator(
    task_id='ETL Mapreduce',
    bash_command='python3 /home/erale/dags/script/mapreduce.py',
    dag=dag)

t4 = DummyOperator(
    task_id='Stop')

t1 >> t2 >> t3 >> t4