# DBTTutorial

In this tutorial, we will show how to use dbt to do some data transformation

## 1. Introduction

### 1.1 What is dbt?
Dbt is a tool which is designed to solve for the T part of ETL, by working on raw data already present in a data 
warehouse.

In recent years, Data warehouses have become extremely flexible(UDFs,etc) and powerful. Features like separation 
of storage and processing, elastic scaling and Machine Learning capabilities(Bigquery’s ML) led many companies to 
use the data warehouse to perform the data transformation and load part of the ETL process (otherwise know as ELT). 

As **dbt provides an easy, version controlled way of writing transformations using just SQL, and data quality check 
natively**, dbt become very popular.

## 2. Install dbt

To install dbt, you can follow the official doc [here](https://docs.getdbt.com/dbt-cli/install/pip). As I use poetry, 
so for me the commands are

```shell
# the dbt-core is mandatory
poetry add dbt-core

# the connector is optional, you only need to install the connector that you need
# for mart I will use postgres as db in this tutorial, so I install 
poetry add dbt-postgres
```


## 3 create a dbt project

To create a dbt project, use below command: (dbt_project is the name of your project, you can name it as you want)

```shell
dbt init my_dbt_project
```

After you entered the command, it will ask you which type of database server you will connect to. You need to install
the needed db connector before you create a project. If the connector is installed, you should see it's number. Then
entered the number in the prompt.

### 3.1 Project layout
Normally it will generate many folders and files:

```text
├── logs
│ └── dbt.log
├── my_dbt_project
│ ├── analyses
│ ├── data
│ ├── dbt_project.yml
│ ├── macros
│ ├── models
│    │   └── example
│    ├── README.md
│    ├── snapshots
│    └── tests

```

- logs : is the folder to store the logs of dbt, when you run a dbt command via CLI, it will create a logs folder.
- my_dbt_project: is the folder to store all the information about my dbt project
    - analyses: You can store sql query for analyzing your data
    - data: You can store some data you need to load into your database
    - dbt_project.yml: is the main configuration file of your project. We will examine it with details
    - macros: Dbt allows users to create macros, which are sql based functions. These macros can be reused across the 
              project.
    - models: **model is the most import concept in dbt.** Ea

### 3.2 The project config file

As we mentioned before, dbt_project.yml is the project config file. It contains:
- project name, version, config-file version
- profile: specify which credential will be used to connect to the target database server
- *-paths: specify where dbt should look for different types of files.
- models: specify how the models that you defined in your project will be materialized in your database server. It 
         could be a table or a view.
```yaml

# Name your project! Project names should contain only lowercase characters
# and underscores. A good package name should reflect your organization's
# name or the intended use of these models
name: 'my_dbt_project'
version: '1.0.0'
config-version: 2

# This setting configures which "profile" dbt uses for this project.
profile: 'my_dbt_project'

# These configurations specify where dbt should look for different types of files.
# The `model-paths` config, for mart, states that models in this project can be
# found in the "models/" directory. You probably won't need to change these!
model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"  # directory which will store compiled SQL files
clean-targets:         # directories to be removed by `dbt clean`
  - "target"
  - "dbt_packages"


# Configuring models
# Full documentation: https://docs.getdbt.com/docs/configuring-models

# In this mart config, we tell dbt to build all models in the mart/ directory
# as tables. These settings can be overridden in the individual model files
# using the `{{ config(...) }}` macro.
models:
  my_dbt_project:
    # Config indicated by + and applies to all files under models/mart/
    example:
      +materialized: view

```

## 4. load data

### 4.1 Set up db credentials in profiles.yml

To load data, you need to set up credential for connection to database server. By default, dbt stores these credentials
in **~/.dbt/profiles.yml**. 

Below is an example of the generated profiles.yml. It can contain multiple profiles(credentials). Each profile starts with the
name, then followed by a dev and prod credentials. Note for each project you created, dbt will generate a profile for
the project. 

```yaml
# name of the profile
my_dbt_project:
  outputs:

    dev:
      type: postgres
      threads: [1 or more]
      host: [host]
      port: [port]
      user: [dev_username]
      pass: [dev_password]
      dbname: [dbname]
      schema: [dev_schema]

    prod:
      type: postgres
      threads: [1 or more]
      host: [host]
      port: [port]
      user: [prod_username]
      pass: [prod_password]
      dbname: [dbname]
      schema: [prod_schema]

  target: dev

```

You need to edit this file by adding the credentials. Below is an example after adding credentials
```yaml
config:
  send_anonymous_usage_stats: False

# note even though we have set two credential dev and prod. But as we set target is dev.
# so only the dev credential will be used.
local_postgres:
  outputs:

    dev:
      type: postgres
      threads: 1
      host: 127.0.0.1
      port: 5432
      user: pliu
      pass: changeMe
      dbname: dbt_project
      schema: public

    prod:
      type: postgres
      threads: 8
      host: 10.20.30.18
      port: 5432
      user: pliu
      pass: changeMe
      dbname: prod_dbt_project
      schema: public

  target: dev

```

### 4.2 Load static csv file

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

### 4.3 Models to load data

Create a folder staging in models, then creat a load_customer_csv.sql file

```sql
-- The source table will read the customers.csv 
with source as (
    select *
    from {{ ref('customer') }}
),
-- The stage_customer table 
stage_customer as (  
    select customer_id,
        zipcode,
        city,
        state_code,
        datetime_created::TIMESTAMP AS datetime_created,
        datetime_updated::TIMESTAMP AS datetime_updated,
    from source
)
select *
from stage_customer

```