from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import string

# Setting the Spark configuration so that the bible WordCount is run locally
conf = SparkConf().setMaster('local').setAppName('Assignment6Solution1')
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

# Reading 1st paragraph file
paragraphA = sc.textFile("file:///home/joe/Assignment6/Solution1/ParagraphA.txt")
# Reading 1st paragraph file
paragraphB = sc.textFile("file:///home/joe/Assignment6/Solution1/ParagraphB.txt")



# Tokenizing each line and keep "normal" characters 
# so we filter digits and punctuations 
# and lower case the line and take away spaces at the beginning and the end 
wordsParaA = paragraphA.flatMap(lambda line: (str(line.lower()).translate(None,string.punctuation).translate(None,string.digits).strip().split()))

# Tokenizing each line and keep "normal" characters 
# so we filter digits and punctuations 
# and lower case the line and take away spaces at the beginning and the end 
wordsParaB = paragraphB.flatMap(lambda line: (str(line.lower()).translate(None,string.punctuation).translate(None,string.digits).strip().split()))

#show amount of words from A 
print "Amount of words in A : " + str(wordsParaA.count())

# show first 10 words from A
print "Here are 10 examples for A:"
for word in wordsParaA.take(10):print word

#show amount of words from B 
print "Amount of words in B : " + str(wordsParaB.count())

# show first 10 words from B
print "Here are 10 examples for B:"
for word in wordsParaB.take(10):print word

# unique words (each word only one time in list) for A and B
uniqueWordsParaA=wordsParaA.distinct()
uniqueWordsParaB=wordsParaB.distinct()

#Output for unique words (A and B) 
print "Amount of unique words in A : " + str(uniqueWordsParaA.count())

# show first 10 words from A
print "Here are 10 examples for unique words from A:"
for word in uniqueWordsParaA.take(10):print word

#show amount of words from B 
print "Amount of unique words in B : " + str(uniqueWordsParaB.count())

# show first 10 words from B
print "Here are 10 examples for unique words from B:"
for word in uniqueWordsParaB.take(10):print word

# words only in A and not in B
WordsOnlyInA=uniqueWordsParaA.subtract(uniqueWordsParaB)

#Output for words only in A
print "Amount of unique words only in A : " + str(WordsOnlyInA.count())

# show first 10 words only in A
print "Here are 10 examples for unique words only in A:"
for word in WordsOnlyInA.take(10):print word

# words which are in both paragraphs
WordsInBoth=uniqueWordsParaA.intersection(uniqueWordsParaB)

#output
print "Amount of unique words in both paragraphs : " + str(WordsInBoth.count())
print "Here are 10 examples for unique words in both paragraphs:"
for word in WordsInBoth.take(10):print word

