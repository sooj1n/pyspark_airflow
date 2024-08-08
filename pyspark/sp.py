"""SimpleApp.py"""
from pyspark.sql import SparkSession
import sys

APP_NAME = sys.argv[1]
LOAD_DT = sys.argv[2]

spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

# 

logFile = "/home/sujin/app/spark-3.5.1-bin-hadoop3/README.md"  # Should be some file on your system
logData = spark.read.text(logFile).cache()

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

#

spark.stop()
