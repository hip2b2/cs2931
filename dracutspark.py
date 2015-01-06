#!/usr/bin/python
# wyu@ateneo.edu
# to run me: spark-submit --num-executors 5 dracutspark.py
#
from pyspark import SparkContext
import re

def extract_end(line):
  m = re.match(".*/([A-Za-z0-9.-]*)'$", line)
  if (m): 
    return m.group(1)

sc = SparkContext(appName="Dracut Spark App")
file = sc.textFile("hdfs://localhost/user/wyy/dracut/dracut.log")
#counts = file.flatMap(lambda line: line.split(" ")) \
#  .map(lambda word: (word, 1)) \
#  .reduceByKey(lambda a, b: a + b)
counts = file.map(lambda line: (extract_end(line), 1)) \
  .reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile("hdfs://localhost:8020/user/wyy/dracut_spark.output")
