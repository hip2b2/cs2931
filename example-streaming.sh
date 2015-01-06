#!/bin/bash
hadoop fs -rm -r -f /user/wyy/streaming/example-streaming.out

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
 -files "example-streaming-mapper.py,example-streaming-reducer.py" \
 -input "/user/wyy/dracut/dracut.log"  \
 -mapper "example-streaming-mapper.py"  \
 -reducer "example-streaming-reducer.py"  \
 -output "/user/wyy/streaming/example-streaming.out"   
