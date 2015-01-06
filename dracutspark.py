#!/usr/bin/python
# wyu@ateneo.edu
# to run me: 
#

# Make sure you have the dependencies ready for this and run it afterwards.
# 1. Install spark. You can do this by typing 'yum install spark pyspark'
# 2. Create the directory for the raw dracut logs needed in this example as the appropriate user. In this case, it is the wyy user. 'hadoop fs -mkdir /user/wyy/dracut'
# 3. Upload the raw dracut.log file to the proper location as the appropriate user. In this case, it is the wyy user. 'hadoop fs -put dracut.log /user/wyy/dracut/'
# 4. Run this script by typing 'spark-submit --num-executors 5 dracutspark.py'


from pyspark import SparkContext
import re

def extract_end(line):
  m = re.match(".*/([A-Za-z0-9.-]*)'$", line)
  if (m): 
    return m.group(1)

sc = SparkContext(appName="Dracut Spark App")
file = sc.textFile("hdfs://localhost/user/wyy/dracut/dracut.log")
counts = file.map(lambda line: (extract_end(line), 1)) \
  .reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile("hdfs://localhost:8020/user/wyy/dracut_spark.output")
