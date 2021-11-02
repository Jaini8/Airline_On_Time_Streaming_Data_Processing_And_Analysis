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
    
    A = data \
        .groupBy('Year', 'Month').agg(F.sum('penalty').alias('num_bad_flights')) \
        .select(['Year', 'Month', 'num_bad_flights']) \
        .withColumn('ID', F.hash('Year', 'Month'))
    
    B = data \
        .groupBy('Year', 'Month').agg(F.count('ArrDelay').alias('num_total_flights')) \
        .select(['Year', 'Month', 'num_total_flights']) \
        .withColumn('ID', F.hash('Year', 'Month'))
    
    non_punc_col = (F.col('num_bad_flights') / F.col('num_total_flights')).alias('non_punctuality')
    C = A \
        .join(B, 'ID', 'inner') \
        .select(A.Year, A.Month, non_punc_col)
        
    send_data(C.toPandas().to_json(orient="split"))


if __name__ == '__main__':
    
    spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    sqlc = SQLContext(spark)

    data = spark \
        .readStream \
        .format("parquet") \
        .option("header", True)\
        .schema(schema)\
        .option("maxFilesPerTrigger", 1)\
        .load("/common/users/shared/cs543_fall21_group6/data/per_year_split/*/")


    penalty_cond = (F.col('ArrDelay') > 30) | (F.col('Cancelled') > 0)
    data = data.withColumn('penalty', F.when(penalty_cond, 1).otherwise(0))
    
    query = data.writeStream.outputMode("update").foreachBatch(batch_split_and_join).start()
    
    time.sleep(10)
    # check for completion and gracefully shutdown
    while query.isActive:
        if not bool(query.status['isDataAvailable']):
            print('Exiting')
            query.stop()

