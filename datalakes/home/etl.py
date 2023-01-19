import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession

## For adding an incremental id to the songplays table
from pyspark.sql.functions import monotonically_increasing_id 

from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

## Read AWS ID and KEY from dl.cfg configuration file
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']

## Creates a Spark session
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

## Process and wrangle song data to generate songs and artists dimension tables
def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df['song_id','title','artist_id','year','duration']
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = songs_table \
                  .write \
                  .partitionBy('artist_id','year') \
                  .parquet(os.path.join(output_data, 'songs.pq'),'overwrite')

    # extract columns to create artists table
    artists_table = df['artist_id','artist_name','artist_location','artist_latitude','artist_longitude'].drop_duplicates()
    
    # write artists table to parquet files
    artists_table = artists_table \
                    .write \
                    .parquet(os.path.join(output_data,'artists.pq'),'overwrite')

## Process and wrangle log data to generate users dimension table, time dimension table and songplays fact table
def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df['userId','firstName','lastName','gender','level'].drop_duplicates()
    
    # write users table to parquet files
    users_table = users_table \
                  .write \
                  .parquet(os.path.join(output_data,'users.pq'),'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x) / 1000)))
    df = df.withColumn("timestamp", get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn("datetime", get_datetime(df.ts))

    # extract columns to create time table
    time_table = df.select(
        'timestamp',
        hour('datetime').alias('hour'),
        dayofmonth('datetime').alias('day'),
        weekofyear('datetime').alias('week'),
        month('datetime').alias('month'),
        year('datetime').alias('year'),
        date_format('datetime', 'F').alias('weekday')
     )  
    
    # write time table to parquet files partitioned by year and month
    time_table = time_table \
                 .write \
                 .partitionBy('year','month') \
                 .parquet(os.path.join(output_data,'time.pq'),'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, 'songs.pq'));
    df_join = df.join(song_df, song_df.title == df.song)
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df_join['timestamp','userId','level','song_id','artist_id','sessionId','location','userAgent']
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table \
                      .repartition(year("timestamp"), month("timestamp")) \
                      .write \
                      .partitionBy("timestamp") \
                      .parquet(os.path.join(output_data,'songplays.pq'),'overwrite')

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
