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
# for example I will use postgres as db in this tutorial, so I install 
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

- logs : is the folder to store the logs of dbt
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
# The `model-paths` config, for example, states that models in this project can be
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

# In this example config, we tell dbt to build all models in the example/ directory
# as tables. These settings can be overridden in the individual model files
# using the `{{ config(...) }}` macro.
models:
  my_dbt_project:
    # Config indicated by + and applies to all files under models/example/
    example:
      +materialized: view

```

## 4. load data

