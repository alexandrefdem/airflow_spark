/*
Select Top 5 devices most used, detected by the useragent of the logs

This query is also available as a view: select * from DW.dbo.top_100_products_country_most_access
*/
with customers_for_most_popular_country as
(select distinct sk_customer, sk_country
    from DW.dbo.factLogs
    where sk_country in
    (select sk_country from
    (select sk_country, ROW_NUMBER() over (order by cnt desc) as rn from
    (select TOP 99999999999 fl.sk_country, count(fl.sk_country) cnt from DW.dbo.factLogs fl group by fl.sk_country) x) y
    where rn = 1) 
)	
select TOP 100 dp.product_name, dc.country_name, cnt as amount_sold
from
(select TOP 9999999999999 sk_product, cs.sk_country, count(sk_product) cnt from
factOrders fo
inner join customers_for_most_popular_country cs on fo.sk_customer=cs.sk_customer
group by sk_product, cs.sk_country
order by count(sk_product) desc) final
inner join dimProduct dp on dp.sk_product = final.sk_product and dp.end_date is null
inner join dimCountry dc on dc.sk_country = final.sk_country and dc.end_date is null
order by 3 desc
	