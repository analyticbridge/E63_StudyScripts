# myWordCount.py	2/25/2016
# Course: CSCI E63 - Big Dat Analytics
# For Homework Assignment 04
# Problem 03 - WordCount program 
# Student: Dongyow Lin

# Import classes (SparkConf, SparkContext, string)
from pyspark import SparkConf, SparkContext
import string

# Set the Spark configurataion to run locally
conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

# Read 4300.txt into RDD

myInputRDD = sc.textFile("/user/joe/ulysis/4300.txt")

# Convert each line into lower case and strip off all punctuation

myInputCleanRDD = myInputRDD.map(lambda line: (str(line.lower()).translate(None,string.punctuation)))
 
# Tokenize each line into word list
myInputWordsRDD = myInputCleanRDD.flatMap(lambda line: line.split(" "))

# Create new tuple with key, value(1) 
myWordListFinalRDD = myInputWordsRDD.map(lambda word: (word, 1))

# Compute the aggregation values for the key and store it in a new result RDD
myWordAggrListRDD = myWordListFinalRDD.reduceByKey(lambda a, b: a+b).sortByKey()

# Output the file to hdfs for all word counts
myWordAggrListRDD.saveAsTextFile("/user/joe/hw04p3/output")



