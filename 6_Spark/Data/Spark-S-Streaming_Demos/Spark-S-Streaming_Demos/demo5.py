#!/usr/bin/python

# Demo - Input Sources - Kafka
#
# This demo will count the records (json objects) placed in a topic test_topic by a 
# Kafka Producer(kafka-console-producer.sh)
# 
#

# Explicitly Point Spark to the Correct Python Executable
# If you already have numpy installed in a specific environment (like conda or a virtualenv), but PySpark is defaulting to /usr/bin/python3 or system Python, 
# explicitly set the Python environment variables before initializing SparkContext:

import os
import sys

# Set these before initializing SparkContext (sc)
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Import Libraries
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Create Spark Session
spark = SparkSession. \
    builder. \
    enableHiveSupport(). \
    appName('Kafka Source'). \
    master('yarn'). \
    getOrCreate()
#spark = SparkSession\
#.builder\
#.master("local")\
#.appName("Kafka Source")\
#.getOrCreate()

# Get the logger and set the log level
spark.sparkContext.setLogLevel("WARN")

# Define Schema
compSchema = StructType([
    StructField("Date", StringType(), True),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Adjusted Close", DoubleType(), True),
    StructField("Script", StringType(), True)
])

# Create Streaming DataFrame by reading data from File Source.
initDF = spark\
.readStream\
.format("kafka")\
.option("kafka.bootstrap.servers", "127.0.0.1:9092")\
.option("subscribe", "test_topic")\
.option("startingOffsets", "earliest")\
.load()
print(initDF.printSchema())
json_stream = initDF\
.select(F.from_json(initDF["value"].cast(StringType()), compSchema).alias("data"))
print(json_stream.printSchema())
json_stream = json_stream.select("data.Date", "data.Open", "data.High", "data.Low",\
"data.Close", "data.Adjusted Close", "data.Script").groupBy().count()

# Output to Console
# Try "update" and "complete" mode.
json_stream\
.writeStream\
.outputMode("complete")\
.option("truncate", False)\
.option("numRows", 3)\
.format("console")\
.start()\
.awaitTermination()



