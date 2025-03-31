
-- Analyze sales performance over time
-- By Year

select YEAR(order_date) as order_year,
SUM(sales_amount) as total_sales,
COUNT(distinct customer_key) as total_customers,
SUM(quantity) as total_quantity
from [dbo].[gold.fact_sales]
where order_date IS NOT NULL
group by YEAR(order_date)
order by YEAR(order_date)

-- By year following months 

select YEAR(order_date) as order_year,
MONTH(order_date) as order_month,
SUM(sales_amount) as total_sales,
COUNT(distinct customer_key) as total_customers,
SUM(quantity) as total_quantity
from [dbo].[gold.fact_sales]
where order_date IS NOT NULL
group by YEAR(order_date), MONTH(order_date)
order by YEAR(order_date), MONTH(order_date)

-- Total sales by month with running total and moving average

select order_date, total_sales, 
sum(total_sales) over(partition by year(order_date) order by order_date) as running_total_sales,
avg(avg_price) over(partition by year(order_date) order by order_date) as moving_avg_price
from 
(select DATETRUNC(month,order_date) as order_date, sum(sales_amount) as total_sales, avg(price) as avg_price
from dbo.[gold.fact_sales] 
where order_date is not null
group by DATETRUNC(month,order_date))t;

/* Analyzing the yearly performance of products by comparing their sales to both
 the average sales performance of the product and the previous year sales */

 with yearly_product_sales as 
 (select year(f.order_date) as order_year ,p.product_name, sum(f.sales_amount) as current_sales
 from [dbo].[gold.fact_sales] f left join [dbo].[gold.dim_products] p
 on f.product_key = p.product_key
 where order_date is not null
 group by year(f.order_date), p.product_name)
 select 
 order_year, product_name, current_sales, 
 avg(current_sales) over (partition by product_name) as avg_sales,
 current_sales - avg(current_sales) over (partition by product_name) as diff_avg,
 case when current_sales - avg(current_sales) over (partition by product_name)  > 0 then 'above avg'
 when current_sales - avg(current_sales) over (partition by product_name)  < 0 then 'below avg'
 else 'avg' end as avg_change,
 lag(current_sales) over (partition by product_name order by order_year) as py_sales,
 current_sales -lag(current_sales) over (partition by product_name order by order_year) as diff_py_sales,
 case when current_sales - lag(current_sales) over (partition by product_name order by order_year) > 0 then 'increase'
 when current_sales - lag(current_sales) over (partition by product_name order by order_year) < 0 then 'decrease'
 else 'no_diff' end as py_change 
 from yearly_product_sales
 order by product_name, order_year;

 -- Which categories contribute the most to overall sales 

 with category_sales as (select p.category, sum(f.sales_amount) as total_sales
 from [dbo].[gold.fact_sales] f
 left join [dbo].[gold.dim_products] p 
 on f.product_key = p.product_key 
 group by category)
 
 select category, total_sales,
 sum(total_sales) over() as overall_sales,
 concat(round((cast (total_sales as float)/ sum(total_sales) over())* 100, 2), '%') as pct_of_total
 from category_sales
 order by total_sales desc;

 -- Segment data into cost ranges and count the number of products fall into that category

 with product_segments as (select product_key, product_name, cost,
 case when cost < 100 then 'Below 100'
 when cost between 100 and 500 then '100-500'
 when cost between 500 and 1000 then '500-1000'
 else 'Above 1000'
 end as cost_range 
 from dbo.[gold.dim_products])
 
 select cost_range,
 COUNT(product_key) as total_products
 from product_segments
 group by cost_range
 order by total_products;

 /* Group customers into three segments based on their spending behavior 
 and find the total number of customers by each segment */

 with customer_spending as 
 (select c.customer_key, sum(f.sales_amount) as total_spending,
 min(f.order_date) as first_order,
 max(f.order_date) as last_order,
 DATEDIFF(month, MIN(order_date), max(order_date)) as lifespan
 from dbo.[gold.fact_sales] f 
 left join dbo.[gold.dim_customers] c
 on f.customer_key = c.customer_key
 group by c.customer_key)

 select customer_segment, count(customer_key) as total_customers
 from
	(select customer_key, total_spending, lifespan,
	case 
	when lifespan >= 12 and total_spending <=5000 then 'Regular'
	when lifespan >=12 and total_spending >5000 then'VIP' 
	else 'New'end as customer_segment 
	from customer_spending)t
 group by customer_segment
 order by total_customers;

 -- Identify loyal customers who made purchases in at least 3 different years

SELECT customer_key, COUNT(DISTINCT YEAR(order_date)) AS purchase_years
FROM dbo.[gold.fact_sales]
GROUP BY customer_key
HAVING COUNT(DISTINCT YEAR(order_date)) >= 3
ORDER BY purchase_years DESC;

-- Analyze which seasons (Spring, Summer, Fall, Winter) generate the most sales.

-- Seasonal sales performance
SELECT 
    CASE 
        WHEN MONTH(order_date) IN (3, 4, 5) THEN 'Spring'
        WHEN MONTH(order_date) IN (6, 7, 8) THEN 'Summer'
        WHEN MONTH(order_date) IN (9, 10, 11) THEN 'Fall'
        ELSE 'Winter'
    END AS season,
    SUM(sales_amount) AS total_sales,
    COUNT(order_number) AS total_orders,
    AVG(sales_amount) AS avg_sales
FROM dbo.[gold.fact_sales]
GROUP BY 
    CASE 
        WHEN MONTH(order_date) IN (3, 4, 5) THEN 'Spring'
        WHEN MONTH(order_date) IN (6, 7, 8) THEN 'Summer'
        WHEN MONTH(order_date) IN (9, 10, 11) THEN 'Fall'
        ELSE 'Winter'
    END
ORDER BY total_sales DESC;




 /* =============================== * CUSTOMER REPORT * ===============================
 Purpose:-
   - This report consolidates key customer metrics and behaviors

 Highlights:-
   1. Gathers essential fields such as names, ages, and transaction details
   2. Segment customers into categories and age groups 
   3. Aggregate customer level metrics 
   - total orders
   - total sales 
   - total quantity purchased 
   - total products
   - lifespan
   4. Calculate KPIs
   - average order value
   - average monthly spend
   - recency ( months since last order)
   */

-- Base query 

with base_query as 
(select f.order_date, f.sales_amount, f.quantity, f.order_number, f.product_key,
c.customer_key, c.customer_number,  
concat (c.first_name, ' ' ,c.last_name) as name, 
datediff (year, c.birthdate, getdate()) as age
from dbo.[gold.fact_sales] f 
left join dbo.[gold.dim_customers] c 
on f.customer_key = c.customer_key
where order_date is not null),

-- Customer aggregations 

customer_aggregations as (select customer_key, customer_number, name, age, 
count(distinct order_number) as total_orders,
sum(sales_amount) as total_sales,
sum(quantity) as total_quantity,
count(distinct product_key) as total_products,
max(order_date) as last_order_date,
datediff(month, min(order_date), max(order_date)) as lifespan
from base_query
group by customer_key, customer_number, name, age)

select customer_key, customer_number, name, age,
case
	when age <20 then 'under 20'
	when age between 20 and 39 then '20-39'
	when age between 40 and 59 then '40-59'
	else '60 and above' end as age_group ,
case 
	when lifespan >= 12 and total_sales <=5000 then 'Regular'
	when lifespan >=12 and total_sales >5000 then'VIP' 
	else 'New'end as customer_segment ,
last_order_date,

-- Compute recency
datediff(month, last_order_date, getdate()) as recency,

-- Compute avg_order_value
case 
when total_orders = 0 then '0' 
else total_sales/total_orders end as avg_order_value,

-- Compute avg monthly spending
case
when lifespan = 0 then total_sales
else total_sales/lifespan end as avg_monthly_spending,

total_orders, total_products, total_quantity, total_sales, lifespan
from customer_aggregations;


/* ================================== PRODUCT REPORT =================================
 Purpose:-
  This report consolidates key product metrics and behaviors.

 Highlights:-
  1. Gathers essential fields such as product name, category, sub-category, and cost.
  2. Segments product revenue by high, mid, and low performers.
  3. Aggregate product level metrics
   - total orders
   - total sales
   - total quantity sold
   - total customers distinct 
   - lifespan
  4. Calculate valuable KPIs
   - recency (month since last sale)
   - average order revenue 
   - average monthly revenue 
*/

with base_querys as 
(select p.product_key, p.product_name, p.category, p.subcategory, p.cost,
f.customer_key, f.quantity, f.sales_amount, f.order_number,f.order_date
from dbo.[gold.fact_sales] f 
left join dbo.[gold.dim_products] p
on f.product_key = p.product_key
where f.order_date is not null),

product_aggregations as 
(select product_key, product_name, category, subcategory, cost,
datediff(month, min(order_date), getdate()) as lifespan,
max(order_date) as last_sale_date,
count(distinct order_number) as total_orders,
count(distinct customer_key) as total_customers,
sum(sales_amount) as total_sales,
sum(quantity) as total_quantity,
round(avg(cast(sales_amount as float)/ nullif(quantity, 0 )), 1) as avg_selling_price
from base_querys
group by product_key,product_name,category,subcategory,cost)

select 
product_key, product_name, category, subcategory, cost, last_sale_date, 
case 
when total_sales > 50000 then 'High performer'
when total_sales >= 10000 then 'Mid performer'
else 'low performer' end as product_segment,
lifespan, total_orders, total_quantity, total_sales, total_customers, 
case 
when total_orders = 0 then 0 
else total_sales/ total_orders end avg_order_revenue,
case
when lifespan = 0 then total_sales
else total_sales/lifespan end as avg_monthly_revenue 
from product_aggregations


