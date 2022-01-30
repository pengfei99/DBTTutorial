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

## 2. Main features

Below are a list of dbt main features:

- sources: It allows you to name and describe the data (database, tables) already in  your warehouse. 
           By declaring these tables as sources in dbt, you can then:
    - select from source tables in your models using the {{ source() }} function, helping define the lineage of your data
    - test your assumptions about your source data
    - calculate the freshness of your source data
- seeds: It allows you to upload csv files to the database server as a table. Note **it's not recommended using this feature
        as data loading tool. It's designed for small static table that you need for data referencing or join**
- snapshots: It records changes to a mutable table over time. And allow us to track past events.
- models: **model is the most import concept in dbt.** Model is a select statement that contains the logic of 
          data transformation. Each model is defined in .sql files (in models directory):
    - The name of the file is used as the model name
    - Models can be nested in subdirectories within the models directory
    - When you execute the dbt run command, dbt will build this model in your data warehouse by wrapping it in a 
      create view as or create table as statement.
- tests: are assertions you make about your models and other resources in your dbt project. you can test any models, 
         sources, and seeds
- documentations: are documentations of your models/sources/seeds, and columns. 
- exposures: It allows you to define and describe a downstream use of your dbt project, such as in a dashboard, 
             application, or data science pipeline. By defining exposures, you can then:
    - run, test, and list resources that feed into your exposure
    - populate a dedicated page in the auto-generated documentation site with context relevant to data consumers
- analyses: For some heavy weight sql analytical query, you don't want to execute them when you run "dbt run", you can
       put these .sql in **analyses** folder. The syntax is exactly the same as model, you can use ref() and source().
       They will be compiled to sql query after you run "dbt compile". You can find the generated sql under
       target/compiled/{project name}/analyses/*.sql. You can copy it in another tool to run the sql query. Nothing
       will be materialized in the database via dbt.

Other features, that I did not show in this tutorial. For more details, you can visit the official [doc](https://docs.getdbt.com/docs/introduction)

- Jinja & Macros
- Hooks & Operations
- Packages
- Metric : is a timeseries aggregation over a table that supports zero or more dimensions. Some examples of metrics include:
    - active users
    - churn rate
    - mrr (monthly recurring revenue)


## 3. Example projects

I use two projects to illustrate dbt above features. 

In **bigquery_project**, first I generate a dbt project with a bigquery connection. Then I use seeds to load csv files, I 
define models that consume these seeds. I also explain the dbt project layout and configuration. At last, I show how to 
use tests to validate models, and how to add documentations. For more details about this project, please read this 
[doc](bigquery_project/README.md)

In **postgres_project**, I created a dbt project with a postgres connection. Then I use sources to reference existing
databases and tables. Then I define snapshots and how to use snapshots to track historical events. And how to use
these events to help your analysis. For more details about this project, please read this [doc](postgres_project/README.md)

