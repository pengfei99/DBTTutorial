# dbt bigquery project

## 1. Create a dbt project

Below command allows you to create a dbt project

```shell
dbt init bigquery_project
```

It will prompt some question based on the database connector that you choose. Based on your answers, it will generate
a database connector config in ~/.dbt/profiles.yaml

For example for the bigquery connector, you should have something similar to below code:
```yaml
gcp_bigquery: # this needs to match the profile: in your dbt_project.yml file
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      keyfile: /home/pliu/creds/dbt-user-gcp-bigquery-sc.json # replace this with the full path to your gcp service account keyfile
      project: my-bigquery-dbt-project # Replace this with your project id
      dataset: dbt_pengfei # Replace this with your dataset name e.g. dbt_bob
      threads: 1
      timeout_seconds: 300
      location: EU
      priority: interactive

```

This file allows dbt to connect to the gcp bigquery API.

To test the validity of the config, you can use
```shell
dbt debug
```

If the config is correct, you should see:
```text
Connection:
  method: service-account
  database: my-bigquery-dbt-project
  schema: dbt_alice
  location: EU
  priority: interactive
  timeout_seconds: 300
  maximum_bytes_billed: None
  execution_project: my-bigquery-dbt-project
  Connection test: [OK connection ok]

```

## 2. Run your dbt model 

As dbt generates some example models to allow you to create views or tables in the database, you can already load some
data into the database server. To do so, run:
```shell
dbt run
```
You will see below output in the terminal:
```text
10:40:19  1 of 2 START table model dbt_pengfei.my_first_dbt_model......................... [RUN]
10:40:21  1 of 2 OK created table model dbt_pengfei.my_first_dbt_model.................... [CREATE TABLE (2.0 rows, 0 processed) in 2.57s]
10:40:21  2 of 2 START view model dbt_pengfei.my_second_dbt_model......................... [RUN]
10:40:22  2 of 2 OK created view model dbt_pengfei.my_second_dbt_model.................... [OK in 1.07s]
```

You can check the gcp bigquery console, you should see you have two tables in your project now.
![bigquery_table_example](../images/dbt_big_query_dataset.PNG)


## 3 Load static csv file

dbt provides a feature called seed to upload a csv file to the database server as a table or view.

You need to do 3 things:

1. check if your dbt_project.yml contains the seed config. You should have below line 
```yaml
seed-paths: ["seeds"]
```
2. Create a folded called **seeds** under the parent root folder, then put your csv file under **seeds**.
3. To upload csv file you need to run
```shell
# this will upload all csv inside the folder
dbt seed

# if you have multiple csv files, but you only want to load a specific csv
dbt seed --select customers.csv

```

You should see new tables created at the bigquery server. Below figure is an example
![dbt_seeds](../images/dbt_seeds.PNG)

**Important note: Pay attention of the csv file name, it can't have the same name with other models. Because they will
all translated into tables/views. The same name will cause conflict**

## 4. Write a custom model

In section 2, we run the generated model to test the project configuration. Now let's write our own model to do some
actual data transformation

### 4.1 Use the seeds in models

As I mentioned before, seeds will be loaded to the target database as a table or view(based on your project configuration).
As a result, it can be referenced as a model.

In below model, we want to calculate the date of first_order, most_recent_order and order_count of each user. As we load
customers.csv and orders.csv as seeds, we can use **{{ ref('customers')}}** and {{ ref('orders') }} to invoke the table.
otherwise, I have to use the full path **my-bigquery-dbt-project.dbt_pengfei.customers**. The reference can avoid not
only many typing, but also a higher level of abstraction of table, I no longer need to pay attention of the table location 
(e.g. project, database_name), dbt will control them for me. 

```sql
with customers as (
    select
        customer_id,
        first_name,
        last_name
    from {{ ref('customers') }}

),
orders as (
    select
        order_id,
        customer_id,
        order_date,
        status

    from {{ ref('orders') }}

),
customer_orders as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders
    from orders
    group by 1

),
final as (
    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders
    from customers
    left join customer_orders using (customer_id)
)

select * from final
```
After you run **dbt run**, you should see a new table/view customer_order_history in the bigquery server. Below figure
is an example

![dbt_customer_order_history](../images/dbt_customer_order_history.PNG)

### 4.2 Change the schema of the table

In this tutorial, we use bigquery as database server, which does not have notion of schema inside a database. But other
database server such as postgresql does have schema. The simplest way is to change **the schema: parameter in your profiles.yml file**.

For multiple schema setup, please visit this [page](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/using-custom-schemas)

## 5. Understand the dbt inner working

You could notice the sql file inside models are not exactly standard sql. It has extra syntax such as reference. So
when you execute **dbt run**, dbt will parse the sql files in models to actual sql file which is compatible with the
target database server (bigquery for this tutorial). 

Most of the time, you don't need to pay attention on the generated sql file, but when you encounter bugs that you don't 
understand, you may check the generated sql to understand which sql query will be actually executed.

There are three locations you can check the generated sql queries:
- In the target/compiled/ directory, you can find compiled select statements
- In the target/run/ directory, you can find compiled create statements
- In the logs/dbt.log file, you can find verbose logging of the query execution.

## 6. Test 

 

