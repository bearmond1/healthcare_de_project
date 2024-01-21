# Overview

This is a rather simple data engineering project, which takes artificial medical admissions data and brings it to star schema data mart. Airflow, running in Docker, controls ETL process, while snowflake serves as a cloud data warehouse. We have 3 layers of data: files with source data, structured source data in snowflake table, and data mart with fact table and dimensions.
Following is the process:
1) Source data is split in two sets of 5 files each, one set is being uploaded in snowflake stage, another one - first in local DB, from there - inserted into snowflake source data table. 
2) After files being staged on snowflake, runs a procedure, processing new files and writes new data in source data table. 
3) After source data table being updated, all the procedures which updates dimensions are being triggered, they check if new values of dimension key was encountered, and if so, write new rows in corresponding dimension.
4) After all dimensions ensured to be up to date, new rows are being inserted into fact table.
5) On top of star schema we have several views, used in powerBI reports like top drugs per medical condition, number of admissions per age group and gender, etc.


# Deploying project 

### Docker
In docker folder resides docker-compose file, which will run docker with MySQL as backend databse, CeleryExecutor, and separate containers for each service. Also it will run MySQL DB for source data, though table for data must be created by hand, script is in dags/fill_local_db_from_files.py . 
In dags/connections.py you will have to provide credentials and other connection details from your snowflake account.

### Snowflake
In snowflake folder there is saved SQL Worksheets from snowflake. They should be executed in following order: source, dimensions, fact_table. Also note that stage cannot be created with a script, so you should do it by hand before setting up everything else. It is possible that some adjustments ought to be made on top of worksheets. 

### Airflow 
After infrastructure is in place, it is time to fill it with data. First of all, local DB with half of the source data. Script is in dags folder, mind the paths. 
With that done, we can run full_dag, or each step separately, if we wanna make sure everything is working smoothly, dags files arranged in such a way that each task has its own dag.

### PowerBI
At this step, i'm not sure what to do :) powerBI file with reports is in corresponding folder, but how to configure new connection for the same data is an open question, you'll have to figure it out.


# Detailed walkthrough

### Setup
First - we run docker, full instructions can be found on airflow docker image page, docker-compose is in docker folder.
![](/screenshots/3_docker_project.png)

Here is an example of source data csv file:
![](/screenshots/1_source_data_sf_short.png)
Full list of columns being: Name,Age,Gender,Blood Type,Medical Condition,Date of Admission,Doctor,Hospital,Insurance Provider,Billing Amount,Room Number,Admission Type,Discharge Date,Medication,Test Results

Files from source_1 - are being uploaded into running in docker MySQL DB
![](/screenshots/4_local_db.png)

After that, dag is ready to run. 
![](/screenshots/5_dag.png)

### Staging files
Files from source_2 folder are being staged at snowflake by running PUT command with python snowflake connector.
![](/screenshots/2_sf_stage.png)
After that snowflake procedure is triggered, which will extract data from new files into table. Processing only new files is archieved by placing stream on stage. In snowflake, stream captures all the changes of its source object, providing opportunity to tell which data comes from new files. 
Here is an example of query for fetching only new data from stage.
![](/screenshots/6_stream_join.png)

Data from local DB are being inserted with python snawflake connector by regular INSERT statement.
Now we have all the data brought into snowflake, represented in a structured way.
![](/screenshots/7_source_data_table.png)


### Updating dimensions and fact table
We should check new records in our source data table for new dimension values. For each dimension we have its own stream, placed on source data table, to track new records. Here is typical task for updating dimension.
![](/screenshots/8_update_task.png)

Usually dimensions are being filled with lots of addition data for dimension key, however, in this project it is only done for Date Dimension - it has columns like DAY_OF_WEEK, WEEK_NUMBER, DAY_NUMBER, YEAR_QUARTER, SEASON, BUISNESS_DAY_FLAG. Buisiness days calendar is downloaded from https://date.nager.at/api/v3/publicholidays/, and loaded into snowflake with dags/load_holidays_info.py script.
![](/screenshots/9_date_dim.png)
In full-blown project we would have had lots data about insurance provider, hospital, medication, etc.

After dimensions in place, we fill our fact table. Here is piece of full view on star schema.
![](/screenshots/10_fact_table.png)


### Views and reports
We have few view on top of our star schema, some of them speaks for themselves, like avg_hospital_duration_per_season, avg_bill_by_gender, avg_bill_by_season, and some used for visual reports.
![](/screenshots/11_report1.png)
![](/screenshots/11_report2.png)
![](/screenshots/11_report3.png)
