import sys
from pyspark import SparkContext, SparkConf

dataFile = "s3://r244-bucket/input.csv"

conf = SparkConf().setAppName("Spark Wordcount")
sc = SparkContext("spark://ip-172-31-30-53.us-west-1.compute.internal:7077", "Wordcount", conf=conf)
textFile = sc.textFile(dataFile)

wordCounts = textFile.flatMap(lambda line: 
    line.split()).map(lambda word: (word,
        1)).reduceByKey(lambda a, b: a+b)
wordCounts.saveAsTextFile("s3://r244-bucket/output")