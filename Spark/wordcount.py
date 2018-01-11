import sys
from pyspark import SparkContext, SparkConf

logFile = "s3://r244-bucket/input.txt"

conf = SparkConf().setAppName("Spark Wordcount")
sc = SparkContext("spark://bigdata-vm:7077", "Wordcount", conf=conf)
textFile = sc.textFile(logFile)

wordCounts = textFile.flatMap(lambda line: 
    line.split()).map(lambda word: (word,
        1)).reduceByKey(lambda a, b: a+b)
wordCounts.saveAsTextFile("s3://r244-bucket/output")