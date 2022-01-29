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

