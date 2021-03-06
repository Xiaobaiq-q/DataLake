import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format,dayofweek,monotonically_increasing_id 


config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    # create a spark session 
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Load song_data from s3 bucket and process it by extracting the songs and artist tables
    and then again loaded back to S3.
    
     Parameters:
            spark       : Spark Session
            input_data  : location of song_data json files 
            output_data : location of dimensional tables will be stored
    """
    # get filepath to song data file
    song_data = input_data+ "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration").dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id') \
                     .parquet(os.path.join(output_data, 'songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude","artist_longitude").dropDuplicates()

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Load log_data from s3 bucket and process it by extracting the users, times and songplays tables
    and then again loaded back to S3.
    
     Parameters:
            spark       : Spark Session
            input_data  : location of log_data json files 
            output_data : location of dimensional and fact tables will be stored
    """
    # get filepath to log data file
    log_data =input_data+ "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page=="NextSong")
    
    # extract columns for users table    
    users_table = df.select("userId","firstName","lastName","gender","level").dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users.parquet'), 'overwrite')
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_timestamp = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    df = df.withColumn('datetime', get_timestamp(df.ts))
    
    # extract columns to create time table
    time_table = df.select(
        col("datetime").alias("start_time"),
        hour("datetime").alias("hour"),
        dayofmonth("datetime").alias("day"),
        weekofyear("datetime").alias("week"), 
        month("datetime").alias("month"),
        year("datetime").alias("year"),
        dayofweek("datetime").alias("weekday")  
    ).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, 'times.parquet'), 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data+"song_data/*/*/*/*.json")
    df=df.join(song_df,song_df.title==df.song,"inner")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.select(
        col("datetime").alias("start_time"),
        col("userId").alias("user_id"),
        "level",
        "song_id",
        "artist_id",
        col("sessionId").alias("session_id"),
        "location",
        col("userAgent").alias("user_agent")
    ).withColumn("songplay_id",monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, 'songplays.parquet'), 'overwrite')


def main():
    """
    1. Create a spark session.
    2. Load song and log data from s3.
    3. Create fact and dimensional table and store them in s3. 
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://xiaobai-s3/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
