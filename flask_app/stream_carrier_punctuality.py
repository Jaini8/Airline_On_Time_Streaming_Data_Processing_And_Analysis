#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql import Window

import requests
import time
import sys
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
    
    if data.rdd.isEmpty():
        spark.stop()
        
    
    A = data \
        .groupBy('Year', 'Month', 'UniqueCarrier').agg(F.sum('is_delayed').alias('num_delayed_flights'))\
        .select(['Year', 'Month', 'UniqueCarrier', 'num_delayed_flights'])\
        .withColumn('ID', F.hash('Year', 'Month', 'UniqueCarrier'))
    
    B = data \
        .groupBy('Year', 'Month', 'UniqueCarrier').agg(F.count('is_delayed').alias('num_total_flights'))\
        .select(['Year', 'Month', 'UniqueCarrier', 'num_total_flights'])\
        .withColumn('ID', F.hash('Year', 'Month', 'UniqueCarrier'))
    
    punc_col = (1 - F.col('num_delayed_flights') / F.col('num_total_flights')).alias('punctuality')
    C = A \
        .join(B, 'ID', 'inner')\
        .select(A.Year, A.Month, A.UniqueCarrier, punc_col)

    C = C.groupBy('Year', 'UniqueCarrier').agg(F.mean('punctuality').alias('punctuality'))
    send_data(C.toPandas().to_json(orient="split"))



if __name__ == '__main__':
    
    spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    
    #Reading Streams
    data = spark \
        .readStream \
        .format("parquet") \
        .option("header", True) \
        .schema(schema) \
        .option("maxFilesPerTrigger", 1) \
        .load("/common/users/shared/cs543_fall21_group6/data/per_year_split/*/")
        # .load("../spark-warehouse/*/")
    
    data = data.withColumn('total_delay', F.col('ArrDelay') + F.col('DepDelay'))
    data = data.withColumn('is_delayed', F.when(F.col('total_delay') > 30, 1).otherwise(0))
    
    query = data.writeStream.outputMode("update").foreachBatch(batch_split_and_join).start()
    
    time.sleep(10)
    # check for completion and gracefully shutdown
    while query.isActive:
        if not bool(query.status['isDataAvailable']):
            print('Exiting')
            query.stop()
