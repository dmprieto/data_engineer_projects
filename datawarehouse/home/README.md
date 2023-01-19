# Data Warehouse with S3 and Redshift
The main goal of this project is to put into practice all the new concepts introduced in the Module "Cloud Data Warehouses" of the Udacity Data Engineer nanodegree program. 

In this case, we assume that there is a company called Sparkify that offers music streaming services and, after collecting data related to the songs that are played by users and the activity of the users when they interact with the application, we want to analyze the data to obtain information about song preferences among users.

## Purpose
Sparkify has 2 main sources of data: logs of user data in JSON format and song information in JSON format. These datasources are stored in different S3 buckets that can be accessed using S3 reading AWS services. For doing this you need to create a role in AWS that has permissions to read from S3.

Because Sparkify has seen an increase in users, the amount of information that they are gathering is growing quickly. So, in order to be able to handle, transform and save this information as the number of user increases, we can use a cloud data warehouse that allows scaling as the data grows. In this project we are going to use Amazon Redshift.


## Database schema design and ETL pipeline
For populating our database we have the following data sets:
1. song dataset: Contains metadata about songs and artists
2. log dataset: Contains metadata that describes the session where an specific user played a particular song, the artist author of the song, user basic info and the timestamp when the song was played

After reviewing the datasets, we can identify 4 possible dimension tables:
1. Songs table: Detailed information of all of songs included in the datasets.
2. Artists table: Detailed information of all of the artists included in the datasets.
3. Users table: User basic information
4. Time table: Correlate a timestamp with the corresponding hour,day,week,month,year and weekday

These dimension tables can enrich our analysis by using a query that joins them with the fact table. For example if we wanted to filter our analysis by user gender, we can join the fact table with the users dimension table and then group by gender and the measurements that we want to include in the analysis.

Another advantage of having these dimension tables is the reduction of duplicate data. For example, in the log files there are multiple entries with the same user info (user_id, first_name, last_name, gender), so instead of maintaing duplicates we create the users table that has just 1 record per user and then we can reference this user as many times as we need in the fact table. This also applies to songs and artists.

Our database schema is based on a star schema optimized for queries on song play analysis. The fact table ‘songplays’ combines data from the song files and the log files, and includes only data that has records with page NextSong. Songplays table also includes some measurements like start_time, level, location and user_agent that can give more insights into our analysis.

Once the database schema was designed, the next step was to create the database and the tables in a Redshift Cluster. The connection to Redshift is handled by python's psycopg2 library. The definition of the 'CREATE','INSERT' and 'DROP' sql statments can be found in sql_queries.py file. 

For automating the table creation process, the file create_tables.py creates a connection to the Redshift Cluster, and then drops and creates the tables. This file should be executed first when testing the project. For running the file open the terminal and type 'python create_tables.py' and press enter.

For the ETL pipeline we are extracting the data from S3 and then inserting it into some staging tables, for the log bucket we created staging_events table and for the song bucket we included in the database the staging_songs table. For inserting the data into the staging tables we are using Redshift's COPY command https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html 

Once the data is copied into each of the staging tables, we use insert/select statements to populate each of the dimensions and the fact table. The ETL process is defined in the etl.py file. For running it you should open the terminal, type 'python etl.py' and press enter. 

Broadly speaking, the ETL process is as follows:
1. A connection to the Redshift cluster is created.
2. The staging tables are populated using the COPY command, that grabs the data inside the S3 buckets and loads the data into the staging tables.
3. For each of the dimension tables and the fact table: run a insert/select statement that cleans and transforms the data from the staging tables into the star schema tables.


## Project structure

**sql_queries.py:** Contains all the create,insert,select and drop SQL sentences used in the project
**create_tables.py:** Manages the connection to the sparkifydb database, dropping the tables and creating them again
**etl.py:** Defines the ETL pipeline to populate the fact table and the dimension tables
**dwh.cfg:** Configuration file for defining all of the properties to connect to the Redshift cluster and to copy the data from S3
**table_screenshots:** Folder with images of the star schema tables

# Screenshots of tables
users table: users_table.png
songs table: songs_table.png
artists table: artists_table.png
time table: time_table.png
songplays table: songplays.png
