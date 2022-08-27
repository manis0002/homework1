-- Big project for SQL
-- Link instruction: https://docs.google.com/spreadsheets/d/1WnBJsZXj_4FDi2DyfLH1jkWtfTridO2icWbWCh7PLs8/edit#gid=0


-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month

select 
   substring(date,1,6) as date, count(totals.visits) visits,sum(totals.pageviews) pageviews,sum(totals.transactions) transactions,sum(totals.totalTransactionRevenue)/power(10,6) Revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
Where date between '20170101' and '20170331'
group by `date`
order by `date`;

-- Query 02: Bounce rate per traffic source in July 2017

select
   trafficSource.source source, count(totals.visits) total_visits,count(totals.bounces) as total_no_of_bounces, count(totals.bounces)/count(totals.visits)*100 bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
where date between '20170701' and '20170731'
group by trafficSource.source
order by count(totals.visits) desc;

-- Query 3: Revenue by traffic source by week, by month in June 2017

select 'Month' as time_type, format_date("%Y%m", parse_date("%Y%m%d", date)) as time, trafficSource.source as source, sum(totals.totalTransactionRevenue)/power(10,6) as Revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
Where date between '20170601' and '20170631'
group by time_type,time,source
union all
select 'Week' as time_type,format_date("%Y%W", parse_date("%Y%m%d", date)) as time, trafficSource.source as source, sum(totals.totalTransactionRevenue)/power(10,6) as Revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
Where  date between '20170601' and '20170631'
group by time_type,time,source
order by Revenue desc;

--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser

  select a.month,a.avg_pageviews_purchase,b.avg_pageviews_non_purchase
  from (select substring(date,1,6) month, sum(totals.pageviews)/(count(distinct fullVisitorId)) as avg_pageviews_purchase
   from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
   where totals.Transactions >=1 and substring(date,1,6) between '201706' and '201707'
   group by month) a
   left join
 (select substring(date,1,6) month, sum(totals.pageviews)/(count(distinct fullVisitorId)) as avg_pageviews_non_purchase
   from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
   where totals.Transactions IS NULL and substring(date,1,6) between '201706' and '201707'
   group by month) b
   on a.month = b.month

-- Query 05: Average number of transactions per user that made a purchase in July 2017

select substring(date,1,6) as `Month`, 
sum(totals.Transactions)/(count(distinct fullVisitorId)) as `Avg_total_transactions_per_user`
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
Where substring(date,1,6) = '201707' and totals.Transactions >=1
group by substring(date,1,6) ;

-- Query 06: Average amount of money spent per session

select substring(date,1,6) as `Month`, 
avg(totals.totalTransactionRevenue) as `avg_revenue_by_user_per_visit`
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
Where substring(date,1,6) = '201707' and totals.Transactions IS NOT NULL
group by substring(date,1,6) ;

-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.

select product.v2ProductName as other_purchased_products,sum(product.productQuantity) as quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
UNNEST(hits) as hits,
UNNEST(hits.product) as product
where fullVisitorId in
(select distinct fullVisitorId, 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
UNNEST(hits) as hits,
UNNEST(hits.product) as product
where product.v2ProductName = "YouTube Men's Vintage Henley" and  substring(date,1,6) = '201707' and product.productRevenue is not null)
and product.v2ProductName <> "YouTube Men's Vintage Henley" 
and substring(date,1,6) = '201707' 
and product.productRevenue is not null
group by other_purchased_products
order by quantity desc;

--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.

select substring(date,1,6) month,  
SUM(CASE WHEN hits.eCommerceAction.action_type = '2' THEN 1 ELSE 0 END) num_product_view,
SUM(CASE WHEN hits.eCommerceAction.action_type = '3' THEN 1 ELSE 0 END) num_addtocart,
SUM(CASE WHEN hits.eCommerceAction.action_type = '6' THEN 1 ELSE 0 END) num_purchase,
SUM(CASE WHEN hits.eCommerceAction.action_type = '3' THEN 1 ELSE 0 END)/SUM(CASE WHEN hits.eCommerceAction.action_type = '2' THEN 1 ELSE 0 END)*100 add_to_cart_rate,
SUM(CASE WHEN hits.eCommerceAction.action_type = '6' THEN 1 ELSE 0 END)/SUM(CASE WHEN hits.eCommerceAction.action_type = '2' THEN 1 ELSE 0 END)*100 purchase_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
UNNEST(hits) hits,
UNNEST(hits.product) product
Where date between '20170101' and '20170331'
group by month
order by month;