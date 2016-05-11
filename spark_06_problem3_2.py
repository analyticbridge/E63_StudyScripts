#Part 2 for Problem 3
# This section of code deals with reading the parquet file from the HDFS and running a query to ensure that the
# the results match results from the previous run before saving the results to the parquet file

#Read parquet file in to a new DataFrame called new_Auction

from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SQLContext, DataFrame

conf = SparkConf().setMaster("local").setAppName("hw6prb3_2")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

new_Auction = sqlContext.read.parquet("hdfs://localhost:8020/user/cloudera/hw6/ebay_parquet/auction.parquet")

#Run a simple query to test
# Querying for number of bids per item

query_results = new_Auction.select(new_Auction["item"], new_Auction["auctionid"]).groupBy(new_Auction["item"]).count().collect()
print "Number of bids per item is:"
print query_results
