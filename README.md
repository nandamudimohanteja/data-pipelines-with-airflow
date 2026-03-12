# Data Engineering - Data Pipelines with Airflow

## Table of Contents
* Definition
    * Project Overview
    * Problem Statement
* Design
    * ETL Design Principles
* Building Pipeline
* How to Run
* Final Result / Analysis
* Software Requirements
* Acknowledgement
* About the Developer

## Definition

### Project Overview
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

### Problem Statement
Sparkify want to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets. 

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of CSV logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Design

### ETL Design Principles
1. Partition Data Tables: Data partitioning can be especially useful when dealing with large-size tables with a long history. When data is partitioned using datestamps, we can leverage dynamic partitions to parallelize backfilling.
2. Load Data Incrementally: This principle makes ETL more modular and manageable, especially when building dimension tables from the fact tables. In each run, we only need to append the new transactions to the dimension table from previous date partition instead of scanning the entire fact history.
3. Enforce Idempotency: Many data scientists rely on point-in-time snapshots to perform historical analysis. This means the underlying source table should not be mutable as time progresses, otherwise we would get a different answer. Pipeline should be built so that the same query, when run against the same business logic and time range, returns the same result.
4. Parameterize Workflow: Just like how templates greatly simplified the organization of HTML pages, Jinja can be used in conjunction with SQL. As we mentioned earlier, one common usage of Jinja template is to incorporate the backfilling logic into a typical Hive query.
5. Add Data Checks Early and Often: When processing data, it is useful to write data into a staging table, check the data quality, and only then exchange the staging table with the final production table. Checks in this 3-step paradigm are important defensive mechanisms - they can be simple checks such as counting if the total number of records is greater than 0 or something as complex as an anomaly detection system that checks for unseen categories or outliers.
6. Build Useful Alerts and Monitoring System: Since ETL jobs can often take a long time to run, it's useful to add alerts and monitoring to them so we do not have to keep an eye on the progress of the DAG constantly. We regularly use EmailOperators to send alert emails for jobs missing SLAs. 

## Building Pipeline
It is often useful to visualize complex data flows using a graph. Visually, a node in a graph represents a task, and an arrow represents the dependency of one task on another. Given that data only needs to be computed once on a given task and the computation then carries forward, the graph is directed and acyclic. This is why Airflow jobs are commonly referred to as "DAGs" (Directed Acyclic Graphs).

Airflow UI allows any users to visualize the DAG in a graph view. The author of a data pipeline must define the structure of dependencies among tasks in order to visualize them. This specification is often written in a file called the DAG definition file, which lays out the anatomy of an Airflow job.

While DAGs describe how to run a data pipeline, operators describe what to do in a data pipeline. Typically, there are three broad categories of operators:
1. Sensors: waits for a certain time, external file, or upstream data source
2. Operators: triggers a certain action (e.g. run a bash command, execute a python function, or execute a Hive query, etc)
3. Transfers: moves data from one location to another

For this project, I have built four different operators that will stage the data, transform the data, and run checks on data quality.

* StageToRedshift Operator: The stage operator is expected to be able to load any JSON and CSV formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters should specify where in S3 the file is loaded and what is the target table. The parameters should be used to distinguish between JSON and CSV file. Another important requirement of the stage operator is containing a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.

* LoadFactOperator: With dimension and fact operators, you can utilize the provided SQL helper class to run data transformations. Most of the logic is within the SQL transformations and the operator is expected to take as input a SQL statement and target database on which to run the query against. You can also define a target table that will contain the results of the transformation.

* LoadDimensionOperator: Dimension loads are often done with the truncate-insert pattern where the target table is emptied before the load. Thus, you could also have a parameter that allows switching between insert modes when loading dimensions. Fact tables are usually so massive that they should only allow append type functionality.

* DataQualityOperator: The final operator to create is the data quality operator, which is used to run checks on the data itself. The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each test, the test result and expected result needs to be checked and if there is no match, the operator should raise an exception and the task should retry and fail eventually. For example, one test could be a SQL statement that checks if certain column contains NULL values by counting all the rows that have NULL in the column. We do not want to have any NULLs so the expected result would be 0 and the test would compare the SQL statement's outcome to the expected result.

## How to Run
Open the terminal and follow the steps below:
1. create_cluster.ipynb
    1. Open the dwh.cfg and provide the AWS access keys and secret.
    2. Launch a redshift cluster using create_cluster.ipynb and create an IAM role that has read access to S3.
    3. Add redshift database info like host, dbname, dbuser, password, and port number, and IAM role info like ARN to dwh.cfg.
2. python create_tables.py
3. python etl.py
4. analysis.ipynb - run all your analysis.

## Final Result / Analysis
Now the Sparkify Analytics team can run multiple queries using the data_analysis.ipynb notebook, or users can connect any tool like Amazon QuickSight, Power BI, or Tableau to the RedShift Cluster. They can perform what-if analysis or slice/dice the data as per their requirement.
1. Currently how many users are listening to songs?
2. How are the users distributed across the geography?
3. Which are the songs they are playing?

## Software Requirements
This project uses the following software and Python libraries:
1. Python 3.0
2. psycopg2
3. Amazon RedShift

You will also need to have software installed to run and execute a Jupyter Notebook. If you do not have Python installed yet, it is highly recommended that you install the Anaconda distribution of Python, which already has the above packages and more included.

## Acknowledgement
Credit is given to Udacity for the project framework and initial datasets. This project is maintained for educational and professional demonstration purposes.

## Bonus: Key Airflow Concepts
1. DAG (Directed Acyclic Graph): a workflow which glues all the tasks with inter-dependencies.
2. Operator: a template for a specific type of work to be executed.
3. Sensor: a type of special operator which will only execute if a certain condition is met.
4. Task: a parameterized instance of an operator/sensor which represents a unit of actual work.
5. Plugin: an extension to allow users to easily extend Airflow with custom hooks and operators.
6. Pools: concurrency limit configuration for a set of Airflow tasks.

## About the Developer
**Mohan Teja Nandamudi**
Data Engineer

Mohan is a Data Engineer with over 6 years of experience building and optimizing enterprise data pipelines and cloud warehouses. He specializes in AWS, Airflow, and Spark, with a focus on delivering scalable, high-performance data architectures for finance and eCommerce platforms.

* Email: mohanteja.0117@gmail.com
* LinkedIn: https://www.linkedin.com/in/nmohant/