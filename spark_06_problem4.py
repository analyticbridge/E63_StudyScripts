#Part 2 for Problem 3
# This section of code deals with reading the parquet file from the HDFS and running a query to ensure that the
# the results match results from the previous run before saving the results to the parquet file

#Read parquet file in to a new DataFrame called new_Auction

from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SQLContext, DataFrame, HiveContext

conf = SparkConf().setMaster("local").setAppName("hw6prb4")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
hivecontext = HiveContext(sc)

#Total the number of items for each order - querying order_items table
total_num_items = hivecontext.sql("select order_item_order_id, sum(order_item_quantity) as total_items from order_items group by order_item_order_id order by total_items DESC ")

#Merge the results from above query and select from orders table
#First register results from above in temp table
total_num_items.registerTempTable("Total_Items")

hive_results = hivecontext.sql("select o.*, t.total_items from orders as o, Total_Items as t where o.order_id=t.order_item_order_id order by t.total_items DESC").take(15)

print " ########### Results from executing the query using hivecontext ############# "
print "Display first 15 orders with high number of total items in the order"
print " "
for each in hive_results:
	print each



