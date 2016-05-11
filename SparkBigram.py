#A bigram is pair of successive tokens in some sequence
#
#set the enviroment 
from pyspark import SparkConf,SparkContext
conf=SparkConf().setAppName("MyHW03")
sc = SparkContext(conf=conf)

lines = sc.textFile("/user/cloudera/4300.txt") 
bigrams = lines.map(lambda x:x.split())\
.flatMap(lambda x: [((x[i],x[i+1]),1) for i in range(0,len(x)-1)])
freqBigrams = bigrams.reduceByKey(lambda x,y:x+y) \
    .map(lambda x:(x[1],x[0])) \
    .sortByKey(False)
freqBigrams.saveAsTextFile("/user/cloudera/FreqBigram")
