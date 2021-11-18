# Data_Bank_CS

```sql
--Question 1 How many unique nodes are there on the Data Bank system?
with base as (select count(distinct node_id) as region_node_counts ,region_id from data_bank.customer_nodes
group by region_id)
select sum(region_node_counts) from base

--2. What is the number of nodes per region?
select region_id, count (distinct node_id) as total_nodes from data_bank.customer_nodes
group by region_id

--Question3 How many customers are allocated to each region?
select region_id, count (distinct customer_id) as total_customer from data_bank.customer_nodes
group by region_id


--Question 4 How many days on average are customers reallocated to a different node?

with tb1 as (SELECT
  customer_id,
  node_id,
  start_date,
  end_date,
  (extract(doy from end_date) - extract(doy from start_date)) ::INTEGER AS duration,
  lead(node_id) OVER (PARTITION BY customer_id ORDER BY start_date) AS lead_rn, 
  lag(node_id) OVER (PARTITION BY customer_id ORDER BY start_date) AS lag_rn
FROM data_bank.customer_nodes),

tb2 as
(select 
  customer_id, 
  node_id, 
  lead_rn,
  lag_rn,
  start_date, 
  (case when node_id = lag_rn then duration + lag(duration) over() else duration end) as duration_m
from tb1)

select (floor(avg(duration_m))) as avg_node_days from tb2 where node_id != lead_rn

--Question 5 What is the median, 80th and 95th percentile for this same reallocation days metric for each region?


--Part B
----1. What is the unique count and total amount for each transaction type?

select txn_type, count(*) as total_records, sum(txn_amount) as total_amount from data_bank.customer_transactions
group by txn_type

--2. What is the average total historical deposit counts and amounts for all customers?
with base as(
select customer_id, count(txn_amount) as deposit_counts, round(avg(txn_amount),2) as avg_deposit
from data_bank.customer_transactions
where txn_type = 'deposit'
group by customer_id)

select floor(avg(deposit_counts)) as avg_deposit_count, ceiling(avg(avg_deposit)) as avg_deposit from base

--3. For each month - how many Data Bank customers make more than 1 deposit and at least either 1 purchase or 1 withdrawal in a single month?

with base as (
select customer_id,txn_type, extract(month from txn_date)as txn_month,
row_number() over(partition by customer_id order by txn_date) as lead_rn from data_bank.customer_transactions
order by customer_id),

month_tb as (select txn_month,
    customer_id,
    sum(case when txn_type = 'deposit' then 1 else 0 end) as deposits,
    sum(case when txn_type = 'purchase' then 1 else 0 end) as purchases,
    sum( case when txn_type = 'withdrawal' then 1 else 0 end) as withdrawals
from base
group by txn_month, customer_id
order by txn_month)

select txn_month, count(customer_id) as customers_count from month_tb
where deposits > 1 and (purchases >= 1 or withdrawals >= 1)
group by txn_month
order by txn_month

--Question 4 What is the closing balance for each customer at the end of the month? Also show the change in balance each month in the same table output.

with base as (
select customer_id, extract(month from txn_date) as txn_month,
(case when txn_type = 'deposit' then txn_amount else 0 end) as deposits,
(case when txn_type = 'withdrawal' then -txn_amount else 0 end) as withdrawals,
(case when txn_type = 'purchase' then -txn_amount else 0 end) as purchases
from data_bank.customer_transactions
order by customer_id),

tb1 as(
select customer_id, txn_month, sum(deposits + withdrawals + purchases) as total_amount
from base 
group by customer_id, txn_month
order by customer_id)

select  customer_id, sum(total_amount) as ending_balance
from tb1 
group by customer_id

```
