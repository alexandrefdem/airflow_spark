	/*
Select Top 5 devices most used, detected by the useragent of the logs

This query is also available as a view: select * from DW.dbo.all_sales_by_month_2021
*/
select distinct TOP 12 month(fo.order_date) as month,year(fo.order_date) as year, sum(fo.price) over (partition by month(fo.order_date),year(fo.order_date)) sum_of_sales
from DW.dbo.factOrders fo
where year(fo.order_date)=2021
order by 1
