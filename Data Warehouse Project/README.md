# Project 3: Data Warehouse
 
This data warehouse project from the Udacity Data Engineering nanodegree creates a example ETL process for sparkify using redshift..

The purpose of this project is to  create a redshift cluster in aws and stage the data there before loading it into the star schema tables.

`Note: To reproduce this  you must put in your own AWS details into the DWH.cfg and launch your own Redshift Cluster`

## Star Schema Design
The star schema has one fact table: "songplays", and four dimensions:"users", "artists", "songs" and "time". The queries to drop, create and insert the the data into the tables is contained within sql_queries.py.

This design allows easy reporting on all areas within the data with each dimension being linked to the songplays table you can easily match artist and user info and report on the portfolio of music subscriptions. 

The data is taken from an s3 bucket and moved to these via 2 staging tables, stage_events and stage_songs. 


![StarSchema](Images/db.png)


## Scripts
The  create_tables.py script firstly drops the tables and database, before recreating the database and tables for staging and the star schema via the scripts in sql_queries.py. 

The etl.py script first copies the data from an s3 bucket into the staging tables, before splitting the event and song files into the star schema tables.

`Note: The etl.py script must be run after create_tables.py`