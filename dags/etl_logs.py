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
    'logs_to_dw',
    schedule_interval='@once',
    start_date=datetime(2021, 1, 29),
    tags=['production'],
    catchup=False,
)

sql_1_process_ipadd_dim = '''
DECLARE @ct DATETIME
DECLARE @tempTable TABLE (ipaddr varchar(200))

set @ct = GETDATE()

--temp table of not existing records on the dimension
INSERT Into @tempTable
select * from 
(select so.ipaddr from Staging.dbo.Logs so
left join (select ipaddr from DW.dbo.dimIpaddr) dc on so.ipaddr = dc.ipaddr
where dc.ipaddr is null ) X

--insert new records and updated records 
insert into DW.dbo.dimIpaddr
select ipaddr, @ct as start_date, NULL as end_date from @tempTable
'''

sql_2_process_country_dim = '''
DECLARE @ct DATETIME
DECLARE @tempTable TABLE (country_name varchar(200))

set @ct = GETDATE()

--temp table of not existing records on the dimension
INSERT Into @tempTable
select * from 
(select distinct country_name from Staging.dbo.Logs so
except
select country_name from DW.dbo.dimCountry dc) X

--insert new records and updated records 
insert into DW.dbo.dimCountry
select country_name, @ct as start_date, NULL as end_date from @tempTable
'''


sql_3_process_device_dim = '''
DECLARE @ct DATETIME
DECLARE @tempTable TABLE (device varchar(200), is_mobile int,  is_pc int, is_tablet int)

set @ct = GETDATE()

--temp table of not existing records on the dimension
INSERT Into @tempTable
select * from 
(select device,is_mobile,is_pc,is_tablet from Staging.dbo.Logs so
except
select device,is_mobile,is_pc,is_tablet from DW.dbo.dimDevice dc) X

--insert new records and updated records 
insert into DW.dbo.dimDevice
select device,is_mobile,is_pc,is_tablet, @ct as start_date, NULL as end_date from @tempTable
'''

sql_4_process_fact_logs ='''
insert into DW.dbo.factLogs
select dimipaddr.sk_ipaddr,dimco.sk_country,dimde.sk_device,dimcu.sk_customer,l.time
from Staging.dbo.Logs l
inner join DW.dbo.dimIpaddr dimipaddr on dimipaddr.ipaddr = l.ipaddr and dimipaddr.end_date is null
inner join DW.dbo.dimCustomer dimcu on dimcu.username = l.username and dimcu.end_date is null
inner join DW.dbo.dimCountry dimco on dimco.country_name = l.country_name and dimco.end_date is null
inner join DW.dbo.dimDevice dimde on
(dimde.device = l.device and dimde.is_mobile = l.is_mobile and dimde.is_pc = l.is_pc and dimde.is_tablet = l.is_tablet) and dimde.end_date is null
'''

t1 = SparkSubmitOperator(
    task_id = "execute_parse_logs_to_staging", 
    application="/root/airflowmount/spark/jobs/parse_logs_from_storage.py",        
    conn_id = 'spark_default',
    queue='queue_1',
    jars = '/root/airflowmount/spark/connectors/mssql-jdbc-9.4.1.jre8.jar',
    driver_class_path = '/root/airflowmount/spark/connectors/mssql-jdbc-9.4.1.jre8.jar',
    dag=dag
)  

t2 = BashOperator(
    task_id = 'move_to_processed',
    queue = 'queue_1',
    dag=dag,
    bash_command='if test -d /root/airflowmount/storage/logs; then current_time=$(date "+%Y.%m.%d-%H.%M.%S") ;mkdir /root/airflowmount/storage/logs/logs_$current_time;mv /root/airflowmount/storage/logs/ /root/airflowmount/storage/logs_processed/logs_$current_time  ;mkdir /root/airflowmount/storage/logs/;fi;'
)

t3 = MsSqlOperator(
    task_id = 'process_ipaddr_dim',
    mssql_conn_id='mssql_default',
    database='B2BPlatform',
    sql=sql_1_process_ipadd_dim,
    dag=dag,
    queue='queue_1'
)

t4 = MsSqlOperator(
    task_id = 'process_country_dim',
    mssql_conn_id='mssql_default',
    database='B2BPlatform',
    sql=sql_2_process_country_dim,
    dag=dag,
    queue='queue_1'
)

t5 = MsSqlOperator(
    task_id = 'process_device_dim',
    mssql_conn_id='mssql_default',
    database='B2BPlatform',
    sql=sql_3_process_device_dim,
    dag=dag,
    queue='queue_1'
)

t6 = MsSqlOperator(
    task_id = 'process_fact_logs',
    mssql_conn_id='mssql_default',
    database='B2BPlatform',
    sql=sql_4_process_fact_logs,
    dag=dag,
    queue='queue_1'
)


t1 >> [t3, t4 , t5] >> t6 >> t2 