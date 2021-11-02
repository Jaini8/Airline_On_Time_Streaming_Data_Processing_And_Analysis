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
    data_source = data \
                .groupBy('Origin', 'Year').count() \
                .select('Origin', 'Year', F.col('count') \
                .alias('source_count'))
    
    data_dest = data \
                .groupBy('Dest').count() \
                .select('Dest', F.col('count') \
                .alias('dest_count'))
    
    data_total = data_source \
                .join(data_dest, data_source.Origin == data_dest.Dest, "outer") \
                .select(data_source.Origin, data_source.source_count, data_dest.dest_count, data_source.Year) \
                .na.fill(value = 0)
    
    data_total = data_total \
                .withColumn("total_count", F.col('source_count') + F.col('dest_count'))
    
    data_final = data_total.select("Origin", 'Year', "total_count")
    
    data_final = data_final \
                .join(airport_data, data_final.Origin == airport_data.iata) \
                .select(airport_data.airport, data_final.Year, data_final.total_count) \
                .orderBy(F.col("total_count").desc()).limit(10)
    
    # data_final.show(10,False)
    
    send_data(data_final.toPandas().to_json(orient="split"))


if __name__ == '__main__':
    
    spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    sqlc = SQLContext(spark)

    airport_data = spark.read.csv("/common/users/shared/cs543_fall21_group6/data/data/airports.csv", header = True)

    data = spark \
        .readStream \
        .format("parquet") \
        .option("header", True)\
        .schema(schema)\
        .option("maxFilesPerTrigger", 1)\
        .load("/common/users/shared/cs543_fall21_group6/data/per_year_split/*/")
    

    query = data.writeStream.outputMode("update").foreachBatch(batch_split_and_join).start()
    
    time.sleep(10)
    # check for completion and gracefully shutdown
    while query.isActive:
        if not bool(query.status['isDataAvailable']):
            print('Exiting')
            query.stop()

