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
    'reprocess_orders_to_dw',
    schedule_interval='@once',
    start_date=datetime(2021, 1, 29),
    tags=['Maintenance','Requires modify DAG'],
    catchup=False,
)

#Here set start and end date for reprocessing. #
# No more changes are needed.                   # 
start_date_reproc = '2021-01-01 00:00:00'
end_date_reproc = '2021-01-10 00:00:00'
#################################################

sql_clear_staging = '''
delete from Staging.dbo.orders_reprocess
'''

sql_extract_orders = f'''
insert into Staging.dbo.orders_reprocess
select o.order_id, o.order_date, co.CUIT,co.name as company_name,cu.*,sc.supplier_id,s.name as supplier_name,sc.product_id,p.name as product_name
,coalesce(cc.catalog_price,sc.price) order_price
from
B2BPlatform.dbo.orders o
inner join B2BPlatform.dbo.companies co on co.CUIT = o.CUIT
inner join B2BPlatform.dbo.customers cu on cu.customer_id = o.customer_id
inner join B2BPlatform.dbo.suppliers_catalog sc on  sc.suppliers_catalog_id = o.suppliers_catalog_id
inner join B2BPlatform.dbo.suppliers s on s.supplier_id = sc.supplier_id
inner join B2BPlatform.dbo.products p on p.product_id = sc.product_id
left join B2BPlatform.dbo.companies_catalog cc on cc.suppliers_catalog_id = sc.suppliers_catalog_id
where o.order_date between cast('{start_date_reproc}' as datetime) and cast('{end_date_reproc}' as datetime)
'''

sql_process_dim_company = '''
DECLARE @ct DATETIME
DECLARE @tempTable TABLE (CUIT int, company_name varchar(300))

set @ct = GETDATE()

--temp table of not existing records on the dimension
INSERT Into @tempTable
select * from 
(select so.CUIT, so.company_name from Staging.dbo.Orders so
except
select dc.CUIT, dc.company_name from DW.dbo.dimCompany dc) X

--if a update arrives, set end_date to now
update DW.dbo.dimCompany set end_date = @ct 
where CUIT in (select CUIT from @tempTable) and end_date is null

--insert new records and updated records 
insert into DW.dbo.dimCompany
select CUIT,company_name, @ct as start_date, NULL as end_date 
from @tempTable
'''

sql_process_dim_customer = '''
DECLARE @ct DATETIME
DECLARE @tempTable TABLE (customer_id int, document_number int, full_name varchar(200), date_of_birth datetime, username varchar(100))

set @ct = GETDATE()

--temp table of not existing records on the dimension
INSERT Into @tempTable
select * from 
(select so.[customer_id], so.[document_number], so.[full_name],so.[date_of_birth],so.[username] from Staging.dbo.Orders so
except
select dc.[customer_id], dc.[document_number], dc.[full_name], dc.[date_of_birth],dc.[username] from DW.dbo.dimCustomer dc) X

--if a update arrives, set end_date to now
update DW.dbo.dimCustomer set end_date = @ct 
where customer_id in (select customer_id from @tempTable)
and end_date is null;

--insert new records and updated records 
insert into DW.dbo.dimCustomer
select customer_id,document_number,full_name,date_of_birth,username,@ct as start_date, NULL as end_date 
from @tempTable
'''

sql_process_dim_supplier = '''
DECLARE @ct DATETIME
DECLARE @tempTable TABLE (supplier_id int, supplier_name varchar(300))

set @ct = GETDATE()

--temp table of not existing records on the dimension
INSERT Into @tempTable
select * from 
(select so.supplier_id, so.supplier_name from Staging.dbo.Orders so
except
select dc.supplier_id, dc.supplier_name from DW.dbo.dimSupplier dc) X

--if a update arrives, set end_date to now
update DW.dbo.dimSupplier set end_date = @ct 
where supplier_id in (select supplier_id from @tempTable) and end_date is null

--insert new records and updated records 
insert into DW.dbo.dimSupplier
select supplier_id,supplier_name, @ct as start_date, NULL as end_date 
from @tempTable
'''

sql_process_dim_product = '''
DECLARE @ct DATETIME
DECLARE @tempTable TABLE (product_id int, product_name varchar(300))

set @ct = GETDATE()

--temp table of not existing records on the dimension
INSERT Into @tempTable
select * from 
(select so.product_id, so.product_name from Staging.dbo.Orders so
except
select dc.product_id, dc.product_name from DW.dbo.dimProduct dc) X

--if a update arrives, set end_date to now
update DW.dbo.dimProduct set end_date = @ct 
where product_id in (select product_id from @tempTable) and end_date is null

--insert new records and updated records 
insert into DW.dbo.dimProduct
select product_id,product_name, @ct as start_date, NULL as end_date 
from @tempTable
'''

sql_clear_factOrders_period= f'''
delete from DW.dbo.factOrders where order_date between cast('{start_date_reproc}' as datetime) and cast('{end_date_reproc}' as datetime)
'''

sql_process_fact_orders = '''
insert into DW.dbo.factOrders
select dimco.sk_company, dimcon.sk_customer, dimls.sk_supplier,dims.sk_product,o.order_price,o.order_date
from Staging.dbo.orders_reprocess o
inner join DW.dbo.dimCompany dimco on dimco.company_name = o.company_name and dimco.end_date is null
inner join DW.dbo.dimCustomer dimcon on dimcon.customer_id = o.customer_id and dimcon.end_date is null
inner join DW.dbo.dimSupplier dimls on dimls.supplier_id = o.supplier_id and dimls.end_date is null
inner join DW.dbo.dimProduct dims on dims.product_id = o.product_id and dims.end_date is null
'''

t1 = MsSqlOperator(
    task_id = 'rep_clear_staging_orders',
    mssql_conn_id='mssql_default',
    database='Staging',
    sql=sql_clear_staging,
    dag=dag,
    queue='queue_1'
)

t2 = MsSqlOperator(
    task_id = 'extract_orders',
    mssql_conn_id='mssql_default',
    database='Staging',
    sql=sql_extract_orders,
    dag=dag,
    queue='queue_1'
)

t3 = MsSqlOperator(
    task_id = 'process_dim_company',
    mssql_conn_id='mssql_default',
    database='Staging',
    sql=sql_process_dim_company,
    dag=dag,
    queue='queue_1'
)

t4 = MsSqlOperator(
    task_id = 'process_dim_customer',
    mssql_conn_id='mssql_default',
    database='Staging',
    sql=sql_process_dim_customer,
    dag=dag,
    queue='queue_1'
)

t5 = MsSqlOperator(
    task_id = 'process_dim_supplier',
    mssql_conn_id='mssql_default',
    database='Staging',
    sql=sql_process_dim_supplier,
    dag=dag,
    queue='queue_1'
)

t6 = MsSqlOperator(
    task_id = 'process_dim_product',
    mssql_conn_id='mssql_default',
    database='Staging',
    sql=sql_process_dim_product,
    dag=dag,
    queue='queue_1'
)

t7 = MsSqlOperator(
    task_id = 'process_fact_orders',
    mssql_conn_id='mssql_default',
    database='Staging',
    sql=sql_process_fact_orders,
    dag=dag,
    queue='queue_1'
)

t8 = MsSqlOperator(
    task_id = 'clear_fact_orders_period',
    mssql_conn_id='mssql_default',
    database='Staging',
    sql=sql_clear_factOrders_period,
    dag=dag,
    queue='queue_1'
)
t1 >> t2 >> [t3,t4,t5,t6] >> t8 >> t7