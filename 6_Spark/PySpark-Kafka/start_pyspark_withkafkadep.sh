#!/bin/bash

# Author : Amit
# Creation Date: 04-09-2026
# Modification Date: 04-09-2026
# Description: A Script to start PySpark Shell with kafka dependency
# Usage:  ./start_pyspark_withkafkadep.sh
# Prerequisit: Hadoop and Docker container for PostgreSQL runnin (source start_all.sh), Spark-Sql shell is not running

export PYSPARK_PYTHON=python3
/opt/spark3/bin/pyspark --master yarn --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 --conf spark.ui.port=0 --conf spark.sql.warehouse.dir=/user/${USER}/warehouse
