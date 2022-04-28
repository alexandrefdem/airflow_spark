import queue
from unicodedata import name
from airflow import DAG
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import pandas as pd
import os
from sqlalchemy import create_engine

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
    'leads_to_dw',
    schedule_interval='@once',
    start_date=datetime(2021, 1, 29),
    tags=['production'],
    catchup=False,
)

def readExcelLoadStaging():
    for root, dirs, files in os.walk('/root/airflowmount/storage/spreadsheets'):
        for file in files:
            if file.endswith('.xlsx'):
                print('Processing file ' + os.path.join(root, file))
                df = pd.read_excel(os.path.join(root, file))
                engine = create_engine('mssql+pyodbc://sa:toptal@host.docker.internal,1533/Staging?driver=/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.8.so.1.1')
                df.to_sql('Leads', con = engine, if_exists = 'replace', chunksize = 5000)


sql_process_contact_dim = '''
DECLARE @ct DATETIME
DECLARE @tempTable TABLE (contact_name varchar(100))

set @ct = GETDATE()

--temp table of not existing records on the dimension
INSERT Into @tempTable
select distinct * from 
(select contact_name from Staging.dbo.Leads so
where contact_name not in (select contact_name from DW.dbo.dimContact)) x

--insert new records and updated records 
insert into DW.dbo.dimContact select contact_name, @ct as start_date, NULL as end_date from @tempTable
'''

sql_process_leadsource_dim = '''
DECLARE @ct DATETIME
DECLARE @tempTable TABLE (lead_source varchar(100))

set @ct = GETDATE()

--temp table of not existing records on the dimension
INSERT Into @tempTable
select distinct * from 
(select lead_source from Staging.dbo.Leads so
where lead_source not in (select lead_source from DW.dbo.dimLeadsource)) x

--insert new records and updated records 
insert into DW.dbo.dimLeadsource select lead_source, @ct as start_date, NULL as end_date from @tempTable
'''
sql_process_status_dim = '''
DECLARE @ct DATETIME
DECLARE @tempTable TABLE (status varchar(100))

set @ct = GETDATE()

--temp table of not existing records on the dimension
INSERT Into @tempTable
select distinct * from 
(select status from Staging.dbo.Leads so
where status not in (select status from DW.dbo.dimStatus)) x

--insert new records and updated records 
insert into DW.dbo.dimStatus select status, @ct as start_date, NULL as end_date from @tempTable
'''

sql_process_fact_leads = '''
insert into DW.dbo.factLeads
select le.contact_date, dimco.sk_company, dimcon.sk_contact,dimls.sk_leadsource,dims.sk_status
from Staging.dbo.Leads le
inner join DW.dbo.dimCompany dimco on dimco.company_name = le.company_name and dimco.end_date is null
inner join DW.dbo.dimContact dimcon on dimcon.contact_name = le.contact_name and dimcon.end_date is null
inner join DW.dbo.dimLeadsource dimls on dimls.lead_source = le.lead_source and dimls.end_date is null
inner join DW.dbo.dimStatus dims on dims.status = le.status and dims.end_date is null
'''


t1 = PythonOperator(
    task_id = 'loadxls_tostaging',
    python_callable=readExcelLoadStaging,
    queue='queue_1',
    dag=dag
)

t2 = BashOperator(
    task_id = 'move_to_processed',
    queue = 'queue_1',
    dag=dag,
    bash_command='if test -d /root/airflowmount/storage/spreadsheets; then current_time=$(date "+%Y.%m.%d-%H.%M.%S") ;mkdir /root/airflowmount/storage/spreadsheets_processed/spreadsheets_$current_time;mv /root/airflowmount/storage/spreadsheets/ /root/airflowmount/storage/spreadsheets_processed/spreadsheets_$current_time  ;mkdir /root/airflowmount/storage/spreadsheets/;fi;'
)

t3 = MsSqlOperator(
    task_id = 'process_contact_dim',
    mssql_conn_id='mssql_default',
    database='Staging',
    sql=sql_process_contact_dim,
    dag=dag,
    queue='queue_1'
)

t4 = MsSqlOperator(
    task_id = 'process_leadsource_dim',
    mssql_conn_id='mssql_default',
    database='Staging',
    sql=sql_process_leadsource_dim,
    dag=dag,
    queue='queue_1'
)

t5 = MsSqlOperator(
    task_id = 'process_status_dim',
    mssql_conn_id='mssql_default',
    database='Staging',
    sql=sql_process_status_dim,
    dag=dag,
    queue='queue_1'
)

t6 = MsSqlOperator(
    task_id = 'process_fact_leads',
    mssql_conn_id='mssql_default',
    database='Staging',
    sql=sql_process_fact_leads,
    dag=dag,
    queue='queue_1'
)

t1 >> [t3,t4,t5] >> t6  >> t2
