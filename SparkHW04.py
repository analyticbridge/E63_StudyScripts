#set the enviroment 
from pyspark import SparkConf,SparkContext
conf=SparkConf().setAppName("MyHW03")
sc = SparkContext(conf=conf)

lines = sc.parallelize("/user/cloudera/4300.txt") 
wordcounts = lines.map( lambda x: x.replace(',',' ').replace('.',' ').replace('-',' ').lower()) \
        .flatMap(lambda x: x.split()) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda x,y:x+y) \
        .map(lambda x:(x[1],x[0])) \
        .sortByKey(False) 
wordcounts.saveAsTextFile ("/user/cloudera/mywordcount")

