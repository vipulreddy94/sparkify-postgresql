
### Creating POSTGRESQL DB for sparkify music app

**Sparkify** is a music app, which has data in the form of **song_files** (containing data about songs and artists) and **log_files** (containing data about which users listen to which songs). 
Data Modeling is used to design a relational database to store data pertaining to this app. 
Python is used to create a postgresql and also to create ETL pipelines to insert data into tables from the files. 


Steps to execute the project. 

1. install and run the postgres docker image and make sure the  correct DB name, user name and pwd name are used in the scripts below.
2. Run the sql_queries.py file, to assign appropriate SQL Queries to variables and lists. 
3. Run create_tables.py file, to connect to the DB created and create the tables.
4. Run etl.py script, to process the data available in data folder make appropriate insertions. This script, is used to create a ETL pipeline - which Extracts data from log files and song files (json files in data folder) and then Transform the data according to requirements and Loads the data into the database tables. 
