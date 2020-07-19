import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import DateType
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dwh.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get("AWS", "KEY")
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get("AWS", "SECRET")


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "/*/*/*/*.json"

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id',
                            'year', 'duration').dropDuplicates(['song_id'])

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy(
        "year", "artist_id").parquet(output_data+'/songs_table')
        
    print('Saving songs_table')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location',
                              'artist_latitude', 'artist_longitude').dropDuplicates(['artist_id'])

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(
        output_data+'/artists_table.parquet')

    print('Saving artists_table.parquet')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "/*.json"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table
    users_table = df.select(
        col("userId").alias("user_id"), 
        col("firstName").alias("first_name"),
        col("lastName").alias("last_name"),
        col("gender"),
        col("level")
        ).drop_duplicates(['user_id', 'level'])

    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data+"/users_table")
    print("saved users table")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(
        x / 1000.0), TimestampType())

    df =  df.withColumn("timestamp", get_timestamp("ts"))

    # extract columns to create time table
    time_table = df.select(
        col("timestamp").alias("start_time"),
        hour(col("timestamp")).alias("hour"),
        dayofmonth(col("timestamp")).alias("day"),
        weekofyear(col("timestamp")).alias("week"),
        month(col("timestamp")).alias("month"),
        year(col("timestamp")).alias("year"),
        dayofweek(col("timestamp")).alias("weekday"),
    ).dropDuplicates(["start_time"])

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy(
        "year", "month").parquet(output_data+'/time_table')
    print('Saving time_table.parquet')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "/songs_table")

    songplays = song_df.join(
        df, (df.artist == song_df.artist_name) & (df.song == song_df.title))
    
    songplays = songplays.withColumn("id", monotonically_increasing_id())

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = songplays.select(
        col("id"),
        col("ts").alias("start_time"), 
        col("userId").alias("user_id"),
        col("level"),
        col("song_id"), 
        col('artist_id'), 
        col("sessionId").alias("session_id"),
        col("location"),
        col("userAgent").alias("user_agent"))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy(
        "year", "month").parquet(output_data+'/songplays_table')
    print('Saving songplays_table')


def main():
    spark = create_spark_session()
    log_data = config.get("S3", "LOG_DATA")
    song_data = config.get("S3", "SONG_DATA")
    output_data = config.get("S3", "OUTPUT_DATA")

    process_song_data(spark, song_data, output_data)
    process_log_data(spark, log_data, output_data)


if __name__ == "__main__":
    main()
