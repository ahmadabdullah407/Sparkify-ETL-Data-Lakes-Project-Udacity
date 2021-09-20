import configparser
from datetime import datetime
import os
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, TimestampType
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Loads song data from base S3 location 
    and extracts JSON data and stores as Parquet
    partitioned by year and artist_id.
    
    Parameters
    ----------
    spark : SparkSession
        Apache Spark session
    input_data : str
        The path prefix for the song data.
    output_data : str
        The path prefix for the output data.
    
    """
    # getting filepath to song data file
    song_data = os.path.join(input_data, 'song_data', '*', '*', '*')
    
    # reading song data file
    df = spark.read.json(song_data)

    # extracting columns to create songs table
    df.createOrReplaceTempView("df_songs")
    songs_table = spark.sql("""SELECT DISTINCT song_id, title, artist_id, year, duration
                            FROM df_songs""")
    
    # writing songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id") \
    .parquet("{}songs".format(output_data))

    # extracting columns to create artists table
    artists_table = spark.sql("""SELECT  DISTINCT artist_id,
                                    artist_name as name,
                                    artist_location as location,
                                    artist_latitude as latitude,
                                    artist_longitude as longitude
                                  FROM df_songs""")
    
    # writing artists table to parquet files
    artists_table.write.mode("overwrite").parquet("{}artists".format(output_data))


def process_log_data(spark, input_data, output_data):
    """
    Loads log event data from base S3 location,
    extracts JSON data.
    Creates users table and stores as Parquet.
    Creates time table and stores as Parquet
    partitioned by year and month.
    Creates songplays table and stores as Parquet
    partitioned by year and month.
    
    Parameters
    ----------
    spark : SparkSession
        Apache Spark session
    input_data : str
        The path prefix for the song data.
    output_data : str
        The path prefix for the output data.
    
    """
    # getting filepath to log data file
    log_data = os.path.join(input_data, 'log_data', '*', '*')

    # reading log data file
    df = spark.read.json(log_data)
    
    # filtering by actions for song plays
    df.createOrReplaceTempView("df_logs")
    df = spark.sql("""SELECT * 
                    FROM df_logs
                    WHERE page = 'NextSong'""")
    df.createOrReplaceTempView("df_logs")

    # extracting columns for users table    
    users_table = spark.sql("""SELECT DISTINCT userId as user_id,
                            firstName as first_name,
                            lastName as last_name,gender,level
                            FROM df_logs""") 
    
    # writing users table to parquet files
    users_table.write.mode("overwrite").parquet("{}users".format(output_data))

    # creating timestamp column from original timestamp column
    spark.udf.register("get_timestamp",lambda x: datetime.fromtimestamp(x / 1000),TimestampType())
    df = spark.sql("""SELECT *, get_timestamp(ts) as start_time
                    FROM df_logs
                    WHERE ts IS NOT NULL""")
    
    # extracting columns to create time table
    df.createOrReplaceTempView("df_logs")
    time_table = spark.sql("""SELECT DISTINCT start_time,
                            hour(start_time) as hour,
                            dayofmonth(start_time) as day,
                            weekofyear(start_time) as week,
                            month(start_time) as month,
                            year(start_time) as year,
                            dayofweek(start_time) as weekday
                            FROM df_logs
                            ORDER BY start_time
                            """)
    
    # writing time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month") \
    .parquet("{}time".format(output_data))
    
    song_data = os.path.join(input_data, 'song_data', '*', '*', '*')
    song_df = spark.read.json(song_data)
    song_df.createOrReplaceTempView("df_songs")
    # extracting columns from joined song and log datasets to create songplays table 
    
    songplays_table = spark.sql("""SELECT monotonically_increasing_id() as songplay_id,
                                    l.start_time,
                                    l.userId as user_id,
                                    l.level,
                                    s.song_id,
                                    s.artist_id,
                                    l.sessionId as session_id,
                                    l.location,
                                    l.userAgent,
                                    year(start_time) as year,
                                    month(start_time) as month
                                FROM df_logs l LEFT JOIN df_songs s
                                ON l.song = s.title AND l.artist = s.artist_name""")
    # writing songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year","month") \
    .parquet("{}songplays".format(output_data))


def main():
    """
    Performs the following roles:
    - Get or create a spark session.
    - Read the song and log data from s3.
    - take the data and transform them to tables
    which will then be written to parquet files.
    - Load the parquet files on s3.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://my-udacity-dend-song-log/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    spark.stop()

if __name__ == "__main__":
    main()
