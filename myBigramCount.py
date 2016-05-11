# myBigramCount.py	2/25/2016
# Course: CSCI E63 - Big Dat Analytics
# For Homework Assignment 04
# Problem 04 - BigramCount program 
# Student: Dongyow Lin

# Import classes (SparkConf, SparkContext, string)
from pyspark import SparkConf, SparkContext
import string

# Set the Spark configurataion to run locally
conf = SparkConf().setMaster("local").setAppName("BigramCount")
sc = SparkContext(conf = conf)

# Read 4300.txt into RDD

myInputRDD = sc.textFile("/user/joe/ulysis/4300.txt")

# Convert each line into lower case and strip off all punctuation except "."
# Need to have "." in place to identify the ending word in each sentence.

stripStr = string.punctuation.replace(".", "")

myInputCleanRDD = myInputRDD.map(lambda line: (str(line.lower()).translate(None,stripStr)))
 
# Tokenize each line into word list
myInputWordsRDD = myInputCleanRDD.flatMap(lambda line: line.split())


# define a bigram() function to return bigram pairs as list

def bigram(words):
   return (tuple(words[i:i+2]) for i in range (0, len(words) - 1)) 
   

 

# Transform the result RDD with lower case and punctuation characters removed to a list and 
# run the bigram function to obtain a list of bigram pairs
myBigramList = list(bigram(myInputWordsRDD.collect()))

# Create a RDD from the resulting bigram pairs list
myBigramListRDD = sc.parallelize(myBigramList)

# Remove those bigram pairs with the first work ending with '.'  i,e, those word from the end of each sentense.
# line[0][-1:] -  line[0] refers to those first word in bigrams, line[0][-1:] refers to the last characters of the first word
# This is to handle the requirement in assigment:
# "However, do not count two words separated by a point at the end of a sentense as a bigram

myBigramListCleanRDD = myBigramListRDD.filter(lambda line: line[0][-1:] is not '.' )

# Now remove all the period '.' from each words in the bigram pairs 
myBigramListNoPeriodRDD = myBigramListCleanRDD.map(lambda word: (word[0].translate(None,string.punctuation), word[1].translate(None,string.punctuation)))

# Create new tuple with key(bigram), value(1) 
myBigramListFinalRDD = myBigramListNoPeriodRDD.map(lambda pair: (pair, 1))

# Compute the aggregation values for the key and store it in a new result RDD
myBigramAggrListRDD = myBigramListFinalRDD.reduceByKey(lambda a, b: a+b).sortByKey()

# Create a new RDD for the first 20 bigrams in the result
myBigramFirst20ListRDD = sc.parallelize(myBigramAggrListRDD.take(20))

# Create a new RDD for all bigrams with the work "heaven" in either words of the pair 
myBigramHeavenListRDD = myBigramAggrListRDD.filter(lambda line: "heaven" in str(line[0])+","+str(line[1]))

# Output the file to hdfs for all bigrams
myBigramAggrListRDD.saveAsTextFile("/user/joe/hw04p4/output1")

# Output the file to hdfs for the first 20 bigrams
myBigramFirst20ListRDD.saveAsTextFile("/user/joe/hw04p4/output2")

# Output the file to hdfs for all bigrams with the word "heaven"
myBigramHeavenListRDD.saveAsTextFile("/user/joe/hw04p4/output3")

