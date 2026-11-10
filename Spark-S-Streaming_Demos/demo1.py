#!/usr/bin/python

# Explicitly Point Spark to the Correct Python Executable
# If you already have numpy installed in a specific environment (like conda or a virtualenv), but PySpark is defaulting to /usr/bin/python3 or system Python, 
# explicitly set the Python environment variables before initializing SparkContext:

import os
import sys

# Set these before initializing SparkContext (sc)
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# First Spark Streaming example using rate source and console sink. 
# Rate source will auto-generate data which we will then print onto a console.

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession. \
    builder. \
    enableHiveSupport(). \
    appName('Rate Source'). \
    master('yarn'). \
    getOrCreate()
#spark = SparkSession. \
#    builder. \
#    enableHiveSupport(). \
#    appName('Rate Source'). \
#    master('yarn'). \
#    getOrCreate()

# Get the logger and set the log level
spark.sparkContext.setLogLevel("WARN")

initDF = spark.readStream.format("rate").option("rowsPerSecond", 1).load()
print(initDF.isStreaming)
print(initDF.printSchema())

resultDF = initDF.withColumn("result", F.col("value") + F.lit(1))

query = resultDF.writeStream.outputMode("append")\
.option("truncate", False).format("console")\
.start()

query.awaitTermination()

