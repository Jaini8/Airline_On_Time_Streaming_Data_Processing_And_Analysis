#!/usr/bin/env python
# coding: utf-8
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.streaming import StreamingContext

import requests
import time
import socket

schema = StructType([
    StructField('Year', IntegerType(), False),
    StructField('Month', IntegerType(), False),
    StructField('DayofMonth', IntegerType(), False),
    StructField('DayofWeek', IntegerType(), False),
    StructField('DepTime', IntegerType(), True),
    StructField('CRSDepTime', IntegerType(), True),
    StructField('ArrTime', IntegerType(), True),
    StructField('CRSArrTime', IntegerType(), True),
    StructField('UniqueCarrier', StringType(), True),
    StructField('FlightNum', IntegerType(), True),
    StructField('TailNum', IntegerType(), True),
    StructField('ActualElapsedTime', IntegerType(), True),
    StructField('CRSElapsedTime', IntegerType(), True),
    StructField('AirTime', IntegerType(), True),
    StructField('ArrDelay', IntegerType(), True),
    StructField('DepDelay', IntegerType(), True),
    StructField('Origin', StringType(), True),
    StructField('Dest', StringType(), True),
    StructField('Distance', StringType(), True),
    StructField('TaxiIn', IntegerType(), True),
    StructField('TaxiOut', IntegerType(), True),
    StructField('Cancelled', IntegerType(), True),
    StructField('CancellationCode', StringType(), True),
    StructField('Diverted', IntegerType(), True),
    StructField('CarrierDelay', IntegerType(), True),
    StructField('WeatherDelay', IntegerType(), True),
    StructField('NASDelay', IntegerType(), True),
    StructField('SecurityDelay', IntegerType(), True),
    StructField('LateAircraftDelay', IntegerType(), True),
])


def send_data(payload):
    requests.post(f'http://{socket.gethostname()}:9996/set_data', json=payload)


def batch_split_and_join(data, epoch):
    
    data_final = data \
        .groupBy("Origin", "Dest", "Year") \
        .count() \
        .select("Origin", "Dest", "Year", F.col('count').alias('total_flights')) \
        .orderBy(F.col("count").desc()).limit(20)

    # data_final = data_final.select("Origin", 'Year', 'Month', "total_count")
    
    
    data_final = data_final \
            .join(airport_origin.select(F.col("iata").alias("origin_code") , F.col("airport").alias("origin_name")), data_final.Origin == F.col("origin_code")) \
            .join(airport_dest, data_final.Dest == airport_dest.iata) \
            .select(F.col("Origin"),F.col("origin_name"), F.col("Dest"),airport_dest.airport, data_final.Year, data_final.total_flights) \
            .orderBy(F.col("total_flights").desc(), data_final.Year)
    
    # data_final.show(20,False)
    
    send_data(data_final.toPandas().to_json(orient="split"))


if __name__ == '__main__':
    
    spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    sqlc = SQLContext(spark)

    airport_data = spark.read.csv("/common/users/shared/cs543_fall21_group6/data/data/airports.csv", header = True)

    airport_origin = airport_data.alias("airport_origin")
    airport_dest = airport_data.alias("airport_dest")

    data = spark \
        .readStream \
        .format("parquet") \
        .option("header", True)\
        .schema(schema)\
        .option("maxFilesPerTrigger", 1)\
        .load("/common/users/shared/cs543_fall21_group6/data/per_year_split/*/")
    
    

    query = data.writeStream.outputMode("update").foreachBatch(batch_split_and_join).start()
    
    time.sleep(5)
    # check for completion and gracefully shutdown
    while query.isActive:
        if not bool(query.status['isDataAvailable']):
            print('Exiting')
            query.stop()

