# HW6 - Problem #1
#Define 2 RDDs and read text from HDFS
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("hw6prb1")
sc = SparkContext(conf=conf)

paragraphA = sc.textFile("hdfs://localhost:8020/user/cloudera/hw6/nytimes.txt")
paragraphB = sc.textFile("hdfs://localhost:8020/user/cloudera/hw6/bbc.txt")

#use flatmap on the RDD to split all sentences in to words
words_P_A = paragraphA.flatMap(lambda x: x.split(" "))
words_P_B = paragraphB.flatMap(lambda x: x.split(" "))

#words_P_A.take(10)
#words_P_B.take(10)

#Find distinct words in each RDD
distinct_words_P_A = words_P_A.distinct()
distinct_words_P_B = words_P_B.distinct()
list_dist_words_P_A = distinct_words_P_A.collect()
list_dist_words_P_B = distinct_words_P_B.collect()


#USe subtract function to find the difference between 2 RDDs
only_in_P_A = distinct_words_P_A.subtract(distinct_words_P_B)
list_only_in_P_A = only_in_P_A.collect()

diff_words_A = only_in_P_A.collect()
list_words_P_B = words_P_B.collect()


count_B=0;
for each in diff_words_A:
	if (each in list_words_P_B):
		count_B+=1

#Find common words to both RDDs using intersection function
common_words = distinct_words_P_A.intersection(distinct_words_P_B).collect()

print "*********************************************************************************"
print "sample of 10 distinct words in paragraphA are: ", list_dist_words_P_A[0:10] 
print "sample 10 distinct words in paragraphB are: ", list_dist_words_P_B[0:10]
print "Sample of 10 words only present in paragraphA but not in paragraph B:", list_only_in_P_A[0:10]
print "Number of words present in paragraphA but not paragraphB:", len(diff_words_A)
print "confirming count of words found in paragraphB that are unqiue to paragraphA (should be 0):" , count_B
print "Sample of 10 common words to both texts are:", common_words[0:10]
print "Total number of common words to both texts are:", len(common_words)
print "*********************************************************************************"
