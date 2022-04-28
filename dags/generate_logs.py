from unicodedata import name
from airflow import DAG
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner':'airflow',
    'depends_on_past':'False',
    'start_date':datetime(2022,1,15),
    'email':['alexandredemagalhaess@gmail.com'],
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':0,
    'retry_delay':timedelta(minutes=1)
}

dag = DAG(
    'generate_logs',
    schedule_interval='@once',
    start_date=datetime(2021, 1, 29),
    tags=['Development'],
    catchup=False,
)

t1 = SparkSubmitOperator(
    task_id = "execute_spark_generate_logs", 
    application="/root/airflowmount/spark/jobs/generate_logs.py",        
    conn_id = 'spark_default',
    queue='queue_1',
    jars = '/root/airflowmount/spark/connectors/mssql-jdbc-9.4.1.jre8.jar',
    driver_class_path = '/root/airflowmount/spark/connectors/mssql-jdbc-9.4.1.jre8.jar',
    dag=dag
)  