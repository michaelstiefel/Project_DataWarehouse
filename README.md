# Project Data Warehouse
This project implements a Redshift data warehouse to enable analytical queries for the startup Sparkify. To this end, an ETL pipeline copies data from S3 buckets into staging tables, and then brings those staging tables into a Star schema. An example Jupyter Notebook shows examples for informative analytical queries that are now straightforward with this Star schema.

## Overview of files
1. sql_queries.py
This file contains the sql-queries that the other two python files -- createtables.py and etl.py -- will use. This means, SQL queries contain all the creation steps for the staging and Star schema tables. It contains the copy commands that copy the S3 bucket into the staging tables. Furthermore, it contains the insert commands which fill in the tables of the Star schema row-by-row from the staging tables. Finally, it also contains drop statements for the tables so createtables can be executed several times if necessary.

2. create_tables.py
This file connects to the redshift database, creates a connection and cursor object and then creates the tables defined in sql_queries (in case they exist before, it drops them first).

3. etl.py
This file connects to the redshift database, creates a connection and cursor object and then runs the copy and insertion queries from sql queries. This way, it creates the staging and the Star schema tables.

4. ExampleAnalyticalQueries.ipynb
This jupyter notebook runs some simple analytical queries that are possible with the Redshift data warehouse. For example: What are the songs that users listened to most often? Which artists have several songs?

## How to use this repo

1.Add information in dwh.cfg to connect to the redshift cluster.
2.Run create_tables.py
3.Run etl.py
4.Run examples from ExampleAnalyticalQueries.ipynb .
