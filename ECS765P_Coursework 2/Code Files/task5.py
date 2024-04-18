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
from pyspark.sql.functions import to_date, count, col, year, month, sum, row_number, avg, dayofmonth, regexp_replace
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
from graphframes import *
from pyspark.sql.functions import concat_ws, col

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
    # ride_df.show(5)


    # taxi_zone_lookup.csv.
    taxi_lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/taxi_zone_lookup.csv")
    clean_taxi_lines = taxi_lines.filter(good_taxi_line)
    taxi_features = clean_taxi_lines.map(lambda l: tuple(l.split(',')))
    header_taxi = taxi_features.first()
    taxi_data_RDD = taxi_features.filter(lambda row: row!=header_taxi)
    column_names_taxi = ["LocationID", "Borough", "Zone", "service_zone"]
    taxi_df = taxi_data_RDD.toDF(column_names_taxi)
    #taxi_df.show(5)

    # Removing double quotes from the taxi_df 
    for column in column_names_taxi:
        taxi_df = taxi_df.withColumn(column, regexp_replace(col(column), '\"', ''))
        
#2}Joining the datasets
    # Performing join on pickup_location and LocationID
    joined_df_pickup = ride_df.join(taxi_df, ride_df.pickup_location == taxi_df.LocationID)
    joined_df_Renamed = joined_df_pickup.withColumnRenamed("Borough", "Pickup_Borough")\
                                 .withColumnRenamed("Zone", "Pickup_Zone")\
                                 .withColumnRenamed("service_zone", "Pickup_service_zone")\
                                 .drop("LocationID")

    # joined_df_Renamed.show(5)
    # joined_df_Renamed.printSchema()

    # Performing join on dropoff_location and LocationID
    joined_df_dropoff = joined_df_Renamed.join(taxi_df, joined_df_Renamed.dropoff_location == taxi_df.LocationID)
    nyc_df = joined_df_dropoff.withColumnRenamed("Borough", "Dropoff_Borough")\
                                 .withColumnRenamed("Zone", "Dropoff_Zone")\
                                 .withColumnRenamed("service_zone", "Dropoff_service_zone")\
                                 .drop("LocationID")
    
#convert the UNIX timestamp to the "yyyy-MM-dd" format
    nyc_df= nyc_df.withColumn("date", date_format(from_unixtime("date"), "yyyy-MM-dd"))

    nyc_df1 = nyc_df.withColumn("date", to_date(nyc_df.date, "yyyy-MM-dd"))
    

    # Extract month from the date    
    nyc_df1 = nyc_df1.withColumn("month", month("date"))

    # Converting string data type to integer type for computation
    nyc_df1 = nyc_df.withColumn("trip_length", col("trip_length").cast(IntegerType()))\
                    .withColumn("driver_total_pay", col("driver_total_pay").cast(IntegerType()))



# TASK 5: Finding anomalies

#1} Extract the data in January and calculate the average waiting time

    # Filter for January
    jan_df = nyc_df1.filter(month("date") == 1)

    # Calculate the average waiting time per day
    avg_waiting_time_jan = jan_df.groupBy(dayofmonth("date").alias("day")) \
                                     .agg(avg("request_to_pickup").alias("average_waiting_time")) \
                                     .orderBy("day")
    # Result
    avg_waiting_time_jan.show(20)
# Create resource object for S3 bucket
    bucket = boto3.resource(
        "s3",
        endpoint_url="http://" + s3_endpoint_url,
        aws_access_key_id=s3_access_key_id,
        aws_secret_access_key=s3_secret_access_key,
    )

    # Store results in S3 bucket
    now = datetime.now()
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")


    output_df = avg_waiting_time_jan.coalesce(1)
    # Form the S3 path for storing the result
    output_path = "s3a://" + s3_bucket + "/aman_" + date_time + "/avg_waiting_time_jan.csv"
    
    # Save the DataFrame to CSV on S3
    output_df.write.csv(path=output_path, mode="overwrite", header=True)

# 2} The day which has the average waiting time exceed 300 seconds

    day_greater_300_secs = avg_waiting_time_jan.filter(col("average_waiting_time") > 300)
    day_greater_300_secs.show()

    spark.stop()






