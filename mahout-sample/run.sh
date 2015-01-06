#!/bin/bash
# Mahout processing example with MovieLens data
# wyu@ateneo.edu

# put data into HDFS for processing
#hadoop fs -mkdir /user/wyy/mahout-sample/
#hadoop fs -put ml-100k.data /user/wyy/mahout-sample/ml-100k.data

# run item-based recommender
mahout recommenditembased --input /user/wyy/mahout-sample/ml-100k.data --output /user/wyy/mahout-sample/recommendations --numRecommendations 100 --outputPathForSimilarityMatrix similarity-matrix --similarityClassname SIMILARITY_COSINE
