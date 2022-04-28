/*
Select Top 5 devices most used, detected by the useragent of the logs

This query is also available as a view: select * from DW.dbo.top_5_devices
*/
select TOP 5 device, is_mobile, is_pc, is_tablet, count(*) log_count
from DW.dbo.factLogs fl
inner join DW.dbo.dimDevice dd on dd.sk_device = fl.sk_device and dd.end_date is null
group by device, is_mobile, is_pc, is_tablet
order by count(*) desc
