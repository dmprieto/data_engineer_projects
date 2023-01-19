# Data Lakes with S3 and Spark
The main goal of this project is to put into practice all the new concepts introduced in the Module "Data Lakes with Spark" of the Udacity Data Engineer nanodegree program. 

We are assuming that there is a company called Sparkify that offers music streaming services and, after collecting data related to the songs that are played by users and the activity of the users when they interact with the application, we want to analyze the data to obtain information about song preferences among users.

## Purpose
Sparkify has 2 main sources of data: logs of user data in JSON format and song information in JSON format. These datasources are stored in different S3 buckets that can be accessed using S3 reading AWS services. For doing this you need to create a role in AWS that has permissions to read from S3.

Because Sparkify has seen an increase in users, the amount of information that they are gathering is growing quickly. In order to be able to handle, transform and save this information as the number of user increases, we can use a Spark cluster to process the data and produce a Data Lake that is going to be persisted in S3.


## Schema design and ETL pipeline
For populating our Data Lake we have the following data sets:
1. song dataset: Contains metadata about songs and artists
2. log dataset: Contains metadata that describes the session where an specific user played a particular song, the artist author of the song, user basic info and the timestamp when the song was played

After reviewing the datasets, we can identify 4 possible dimension tables:
1. Songs table: Detailed information of all of songs included in the datasets.
2. Artists table: Detailed information of all of the artists included in the datasets.
3. Users table: User basic information
4. Time table: Correlate a timestamp with the corresponding hour,day,week,month,year and weekday

These dimension tables can enrich our analysis by using a query that joins them with the fact table. For example if we wanted to filter our analysis by user gender, we can join the fact table with the users dimension table and then group by gender and the measurements that we want to include in the analysis.

Another advantage of having these dimension tables is the reduction of duplicate data. For example, in the log files there are multiple entries with the same user info (user_id, first_name, last_name, gender), so instead of maintaing duplicates we create the users table that has just 1 record per user and then we can reference this user as many times as we need in the fact table. This also applies to songs and artists.

Our schema is based on a star schema optimized for queries on song play analysis. The fact table ‘songplays’ combines data from the song files and the log files, and includes only data that has records with page NextSong. Songplays table also includes some measurements like start_time, level, location and user_agent that can give more insights into our analysis.

Once the schema was designed, the next step was to create the Data Lake using an Apache Spark cluster and S3. For doing so, we used pyspark to manage the connection to Spark, the processing of the data into dataframes and the persistence of the transformed data into parquets stored in S3.

For the ETL pipeline we are extracting the song and log data from S3 to some dataframes, then some data wrangling is done to acommodate the data into the different dimension and fact tables. Finally, each table is persisted as a parquet in a different S3 bucket. The ETL process is defined in the etl.py file. For running it you should open the terminal, type 'python etl.py' and press enter. 

Broadly speaking, the ETL process is as follows:
1. A connection to s Spark cluster is created.
2. The song files are accessed from S3, then they are loaded to dataframes using spark and are filtered to populate songs and artists parquets. The parquets are stored in another bucket in S3
3. The log files are accessed from S3, then they are loaded to dataframes using spark and are filtered to populate users parquet. For populating the time parquet, a series of lambda funtions are used to calculate timestamp column and datetime column from original timestamp column. The parquets are stored in another bucket in S3
4. Finally, after joining the song_data with the log_data into another dataframe, the songplays parquet is persisted to S3. And id is generated for each of the songplays records

## Project structure

**etl.py:** Defines the ETL pipeline to populate the fact table parquet and the dimension tables parquets
**dl.cfg:** Configuration file for defining all of the properties to connect to the Spark cluster and to copy the data from S3

