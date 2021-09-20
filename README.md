# Data Lakes Project Sparkify ETL:
## Project Introduction:
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

My role is to build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables.
## Prerequisites:
This project makes the folowing assumptions:
- Python 3 installed,Python 3 Libraries available are given below:
    - configparser.
    - pyspark.
    - datetime.
    - os.
- The data resides in S3.
- I created my own Amazon EMR cluster but user will need to have his own EMR cluster available on AWS.
## Dataset:
For this project, we shall be working with two datasets that reside in S3. Here are the S3 links for each:

-   Song data:  `s3://udacity-dend/song_data`
-   Log data:  `s3://udacity-dend/log_data`
### Song Dataset

The first dataset is a subset of real data from the  [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json

```

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
````
### Log Dataset

The second dataset consists of log files in JSON format generated by this  [event simulator](https://github.com/Interana/eventsim)  based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.

```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json

```

And below is an example of what the data in a log file, 2018-11-12-events.json, looks like.

![](https://video.udacity-data.com/topher/2019/February/5c6c3ce5_log-data/log-data.png)

## Schema for Song Play Analysis

Using the song and log datasets, I created a star schema optimized for queries on song play analysis.This star schema has 1  _fact_  table (songplays), and 4  _dimension_  tables (users, songs, artists, time). 

[![](https://github.com/kenhanscombe/project-postgres/raw/master/sparkify_erd.png?raw=true)](https://github.com/kenhanscombe/project-postgres/blob/master/sparkify_erd.png?raw=true)
## Project Template:
Files used on the project:

-   `etl.py`  reads data from S3, processes that data using Spark, and writes them back to S3
-   `dl.cfg`contains your AWS credentials
-   `README.md`  provides discussion on your process and decisions


## Deployment

Configure dl.cfg. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY with creedentials that have read write access to S3. 

Copy dl.cfg and etl.py to your EMR cluster

`scp -i <path to your SSH key> etl.py <your emr host>:~/`
`scp -i <path to your SSH key> dl.cfg <your emr host>:~/`

## [](https://github.com/mark-fogle/udacity-dend-datalake-spark#execution)Execution

SSH to EMR cluster.

Submit Apache Spark job.

``/usr/bin/spark-submit --master yarn etl.py``
