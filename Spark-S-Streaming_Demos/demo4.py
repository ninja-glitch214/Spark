#!/usr/bin/python

# Demo - Input Sources - Kafka
# 
# This is a Kafka Streaming word count application

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

# Create Streaming DataFrame by reading data from kafka.
initDF = spark\
.readStream\
.format("kafka")\
.option("kafka.bootstrap.servers", "127.0.0.1:9092")\
.option("subscribe", "test_topic")\
.load()\
.select(F.col("value").cast(StringType()))
print(initDF.printSchema())

# Perform word count on streaming DataFrame
wordCount = initDF\
.select(F.explode(F.split(F.col("value"), " "))\
.alias("words"))\
.groupBy("words")\
.count()

# Try "update" and "complete" mode.
wordCount\
.writeStream\
.outputMode("update")\
.option("truncate", False)\
.format("console")\
.start()\
.awaitTermination()


