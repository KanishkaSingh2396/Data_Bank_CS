# Data_Bank_CS

![image](https://user-images.githubusercontent.com/89623051/143561248-fd3f33aa-08f9-4f94-8bd6-09d77f25a773.png)

### Introduction

There is a new innovation in the financial industry called Neo-Banks: new aged digital only banks without physical branches.

Danny thought that there should be some sort of intersection between these new age banks, cryptocurrency and the data world…so he decides to launch a new initiative - Data Bank!

Data Bank runs just like any other digital bank - but it isn’t only for banking activities, they also have the world’s most secure distributed data storage platform!

Customers are allocated cloud data storage limits which are directly linked to how much money they have in their accounts. There are a few interesting caveats that go with this business model, and this is where the Data Bank team need your help!

The management team at Data Bank want to increase their total customer base - but also need some help tracking just how much data storage their customers will need.

This case study is all about calculating metrics, growth and helping the business analyse their data in a smart way to better forecast and plan for their future developments!

### Available Data

The Data Bank team have prepared a data model for this case study as well as a few example rows from the complete dataset below to get you familiar with their tables.

### Entity Relationship Diagram

![image](https://user-images.githubusercontent.com/89623051/143561498-4845cf66-85e3-434b-afce-2a46ce73435e.png)

### Datasets

### Table 1: Regions

Just like popular cryptocurrency platforms - Data Bank is also run off a network of nodes where both money and data is stored across the globe. In a traditional banking sense - you can think of these nodes as bank branches or stores that exist around the world.

This regions table contains the region_id and their respective region_name values

![image](https://user-images.githubusercontent.com/89623051/143561630-5602b6fc-f4c4-4fc9-aca2-7c99b252d44a.png)

### Table 2: Customer Nodes
Customers are randomly distributed across the nodes according to their region - this also specifies exactly which node contains both their cash and data.

This random distribution changes frequently to reduce the risk of hackers getting into Data Bank’s system and stealing customer’s money and data!

Below is a sample of the top 10 rows of the data_bank.customer_nodes

![image](https://user-images.githubusercontent.com/89623051/143561708-526bb5c1-3779-415b-a150-971407f34f1f.png)

### Table 3: Customer Transactions

This table stores all customer deposits, withdrawals and purchases made using their Data Bank debit card.

![image](https://user-images.githubusercontent.com/89623051/143561829-53fff2c5-3578-4151-8423-a6d37a0e2304.png)

### Case Study Questions

The following case study questions include some general data exploration analysis for the nodes and transactions before diving right into the core business questions and finishes with a challenging final request!

### A. Customer Nodes Exploration

How many unique nodes are there on the Data Bank system?

What is the number of nodes per region?

How many customers are allocated to each region?

How many days on average are customers reallocated to a different node?

What is the median, 80th and 95th percentile for this same reallocation days metric for each region?

### B. Customer Transactions

What is the unique count and total amount for each transaction type?

What is the average total historical deposit counts and amounts for all customers?

For each month - how many Data Bank customers make more than 1 deposit and either 1 purchase or 1 withdrawal in a single month?

What is the closing balance for each customer at the end of the month?

Comparing the closing balance of a customer’s first month and the closing balance from their second nth, what percentage of customers:

Have a negative first month balance?

Have a positive first month balance?

Increase their opening month’s positive closing balance by more than 5% in the following month?

Reduce their opening month’s positive closing balance by more than 5% in the following month?

Move from a positive balance in the first month to a negative balance in the second month?

## CASE STUDY SOLUTION

### Part A. Customer Nodes Exploration


##### Question 1 How many unique nodes are there on the Data Bank system?
```sql
with base as (select count(distinct node_id) as region_node_counts ,region_id from data_bank.customer_nodes
group by region_id)
select sum(region_node_counts) from base
```
![image](https://user-images.githubusercontent.com/89623051/143563304-a556b04d-89d8-4868-94f7-d270633f50d4.png)

##### qUESTION 2. What is the number of nodes per region?
```sql
select region_id, count (distinct node_id) as total_nodes from data_bank.customer_nodes
group by region_id
```
![image](https://user-images.githubusercontent.com/89623051/143563406-f85cc8d6-6843-45c7-8c55-0ae283f02509.png)

##### Question3 How many customers are allocated to each region?
```sql
select region_id, count (distinct customer_id) as total_customer from data_bank.customer_nodes
group by region_id
```
![image](https://user-images.githubusercontent.com/89623051/143563406-f85cc8d6-6843-45c7-8c55-0ae283f02509.png)

##### Question 4 How many days on average are customers reallocated to a different node?
```sql
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
```
![image](https://user-images.githubusercontent.com/89623051/143563737-3b098035-5f47-440e-854f-678157a10ab1.png)

##### Question 5 What is the median, 80th and 95th percentile for this same reallocation days metric for each region?


### Part B

##### 1. What is the unique count and total amount for each transaction type?
```sql
select txn_type, count(*) as total_records, sum(txn_amount) as total_amount from data_bank.customer_transactions
group by txn_type
```
![image](https://user-images.githubusercontent.com/89623051/143563853-f0797d6e-e64a-4233-90fd-3d5503f30695.png)

##### 2. What is the average total historical deposit counts and amounts for all customers?
```sql
with base as(
select customer_id, count(txn_amount) as deposit_counts, round(avg(txn_amount),2) as avg_deposit
from data_bank.customer_transactions
where txn_type = 'deposit'
group by customer_id)

select floor(avg(deposit_counts)) as avg_deposit_count, ceiling(avg(avg_deposit)) as avg_deposit from base
```
![image](https://user-images.githubusercontent.com/89623051/143563991-99ead0df-61d2-4089-a945-930cc8082ee3.png)

##### 3. For each month - how many Data Bank customers make more than 1 deposit and at least either 1 purchase or 1 withdrawal in a single month?
```sql
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
```
![image](https://user-images.githubusercontent.com/89623051/143564125-39b5fa7b-bec9-48fe-9db0-788e3dd58668.png)

##### Question 4 What is the closing balance for each customer at the end of the month? Also show the change in balance each month in the same table output.
```sql
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
![image](https://user-images.githubusercontent.com/89623051/143564255-b12e1385-4106-47c6-845e-1e81039515f6.png)

