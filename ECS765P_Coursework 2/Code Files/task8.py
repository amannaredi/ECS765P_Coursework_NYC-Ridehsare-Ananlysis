import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import from_unixtime, date_format
from pyspark.sql.functions import to_date, count, col, year, month
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from graphframes import *
from pyspark.sql.functions import concat_ws, col, regexp_replace

#SPARK Configuration
if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("NYC")\
        .getOrCreate()
    
    def good_ride_line(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            return True
        except:
            return False
            
    def good_taxi_line(line):
        try:
            fields = line.split(',')
            if len(fields)!=4:
                return False
            #int(fields[0])
            return True
        except:
            return False
  
    
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

#Task 1 Merging Datasets

#1.1 Loading rideshare and taxizone RDD from S3 bucket and converting into dataframes
    # rideshare_data.csv 
    rides = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/rideshare_data.csv")
    clean_rides = rides.filter(good_ride_line)
    ride_features = clean_rides.map(lambda l: tuple(l.split(',')))
    header_ride = ride_features.first()
    ride_data_RDD = ride_features.filter(lambda row: row!=header_ride)
    column_names = ["business", "pickup_location", "dropoff_location", "trip_length", "request_to_pickup", "total_ride_time", 
        "on_scene_to_pickup", "on_scene_to_dropoff", "time_of_day", "date", "passenger_fare", "driver_total_pay", "rideshare_profit", 
         "hourly_rate", "dollars_per_mile"]
    ride_df = ride_data_RDD.toDF(column_names)

    # taxi_zone_lookup.csv.
    taxi_lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/taxi_zone_lookup.csv")
    clean_taxi_lines = taxi_lines.filter(good_taxi_line)
    taxi_features = clean_taxi_lines.map(lambda l: tuple(l.split(',')))
    header_taxi = taxi_features.first()
    taxi_data_RDD = taxi_features.filter(lambda row: row!=header_taxi)
    column_names_taxi = ["LocationID", "Borough", "Zone", "service_zone"]
    taxi_df = taxi_data_RDD.toDF(column_names_taxi)

    # Removing double quotes from the taxi_df 
    for column in column_names_taxi:
        taxi_df = taxi_df.withColumn(column, regexp_replace(col(column), '\"', ''))
        

# Task 8

  #part 1 Define the schema for vertices dataframe based on the taxi zone lookup data
    vertex_Schema = StructType([
        StructField("id", StringType(), False),  # False for nullable indicates this field cannot be null
        StructField("Borough", StringType(), True),
        StructField("Zone", StringType(), True),
        StructField("service_zone", StringType(), True)
    ])

    # Define the schema for edges dataframe based on the rideshare data
    edge_Schema = StructType([
        StructField("src", StringType(), False),
        StructField("dst", StringType(), False)
    ])
    
    
#2} Convert the taxi data RDD to a DataFrame with columns as the vertex schema 
    vertices = taxi_df.select(col("LocationID").alias("id"), col("Borough"), col("Zone"), col("service_zone"))
    
    # Extract the edges from the ride data and convert to a DataFrame with columns as the edge schema
    edges = ride_df.select(col("pickup_location").alias("src"), 
                           col("dropoff_location").alias("dst"))
    
    # Create the GraphFrame
    graph_main = GraphFrame(vertices, edges)
    
    # Show 10 samples of the vertices and edges DataFrame
    graph_main.vertices.show(10)
    graph_main.edges.show(10)

#3} Corrected join operations for graph samples
    # Now print the graph using the show() command on "triplets" properties which return DataFrame with columns ‘src’, ‘edge’, and ‘dst’

    graph_main.triplets.distinct().show(10, truncate=False)


#4} Task 4: connected vertices
    # Finding patterns in the graph where two connected vertices belong to the same Borough and service zone.
    borough_services = graph_main.find("(a)-[e]->(b)") \
        .filter("a.Borough = b.Borough AND a.service_zone = b.service_zone")
    
    # Selecting the columns
    borough_service_vertices = borough_services.select(
        col("a.id"),
        col("b.id"),
        col("a.Borough"),
        col("a.service_zone")
    )
    
    # Showing 10 samples
    # borough_service_vertices.distinct().show(10, truncate=False)
    
    # Counting the total number of such connections
    print("********************************************************************")
    total_connected_vertices = borough_service_vertices.count()
    print(f"Count of total connected vertices with the same Borough and service zone is: {total_connected_vertices}")
    print("********************************************************************")

    # TASK 5
    # Running the PageRank algorthm
    page_rank = graph_main.pageRank(resetProbability=0.17, tol=0.01).vertices.sort('pagerank', ascending=False)
    page_rank.select("id", "pagerank").show(5, truncate=False)

    spark.stop()