# HW6 - Problem #3
#Define 2 RDDs and read text from HDFS
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SQLContext, DataFrame

conf = SparkConf().setMaster("local").setAppName("hw6prb3")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#Function to print text for items with price above 100
def printList(x):
	print " "
	for l in x:
		print l

def func_printBids(x,y):
	i = 0
	print " "
	for each in x:
		if i <y :
			print each
		else :
			break
		i+=1


#Read ebay.txt from HDFS
ebay = sc.textFile("hdfs://localhost:8020/user/cloudera/hw6/ebay.csv")

#Split lines based on comma delimiter and create Row based RDD to feed to DataFrame
ebay_fields = ebay.map(lambda x: x.split(","))

#Apply ROW function to ebay_fields
ebay_rows = ebay_fields.map(lambda r: Row(auctionid=int(r[0]), bid=float(r[1]), bidtime=float(r[2]), bidder=r[3], bidderrate=float(r[4]), openbid=float(r[5]), price=float(r[6]), item=r[7], daystolive=float(r[8])))

#Read in to a dataframe
Auction = sqlContext.createDataFrame(ebay_rows)

#Verify schema of Auction Dataframe
Auction.printSchema()

#Verify number of lines read in to DataFrame
line_count_Auc = Auction.count()

#Answer questions in HW and assign to a variable to print out
total_auctions = Auction.select(Auction["auctionid"]).distinct().count()
bids_per_item = Auction.select(Auction["item"], Auction["auctionid"]).groupBy(Auction["item"]).count().collect()
min_price_per_item = Auction.select(Auction["item"], Auction["price"]).groupBy(Auction["item"]).min().collect() 
max_price_per_item = Auction.select(Auction["item"], Auction["price"]).groupBy(Auction["item"]).max().collect()
avg_price_per_item = Auction.select(Auction["item"], Auction["price"]).groupBy(Auction["item"]).avg().collect()

min_bids_per_item = Auction.select(Auction["auctionid"], Auction["item"]).groupBy(Auction["auctionid"], Auction["item"]).count().groupBy(Auction["item"]).min("count").collect()

max_bids_per_item = Auction.select(Auction["auctionid"], Auction["item"]).groupBy(Auction["auctionid"], Auction["item"]).count().groupBy(Auction["item"]).max("count").collect()

avg_bids_per_item = Auction.select(Auction["auctionid"], Auction["item"]).groupBy(Auction["auctionid"], Auction["item"]).count().groupBy(Auction["item"]).avg("count").collect()

highprice_df = Auction.select('*').filter(Auction["price"] > 100).collect()

print "*********************************************************************************"
print "Number of lines read in to Auction DataFrame: ",line_count_Auc
print " "
print "###### Following queries were executed against the dataframe ######"
print "Total number of Auctions held: ", total_auctions
print " "
print "Bids per items: " , printList(bids_per_item)
print "Minimum price per item: ", printList(min_price_per_item)
print "Maximum price per item: ", printList(max_price_per_item)
print "Average price per item: ", printList(avg_price_per_item)
print "Minimum no. of bids per item: ", printList(min_bids_per_item)
print "Maximum no. of bids per item: ", printList(max_bids_per_item)
print "Average no.of bids per item: ", printList(avg_bids_per_item)
print "Showing 20 bids with price higher than 100"
print func_printBids(highprice_df, 20)

print "*********************************************************************************"
#Register the dataframe as a temp table
Auction.registerTempTable("Auction_Table")

#Answers to questions using sqlcontext
auctions_total = sqlContext.sql("select count (distinct item)  from Auction_Table").collect()

#bids made per item
s_bids_per_item = sqlContext.sql("select count(auctionid), item  from Auction_Table group by item ").collect()
s_min_max_avg_price_per_item = sqlContext.sql("select min(price) as MIN_PRICE, max (price) as MAX_PRICE, avg(price) as AVG_PRICE,  item  from Auction_Table group by item ").collect()

#First query the results of number of bids for each auction and the item involved and store it in a temporary dataframe

df_bids_auction = sqlContext.sql("select item, count(auctionid) as bidcount from Auction_Table group by auctionid, item")

#Create a temporary table for the above dataframe
df_bids_auction.registerTempTable("Bids_Auction_Table")
#Query the new temporary table to get the min, max and avg
s_min_max_avg_bids_per_item = sqlContext.sql("select item, min(bidcount) as MIN_BIDS, max(bidcount) as MAX_BIDS, avg(bidcount) as AVG_BIDS from Bids_Auction_Table group by item").collect()

s_price_high = sqlContext.sql("select *  from Auction_Table where price > 100 ").collect()

print "###### Following queries were executed using SQLContext ######"
print "Total number of Auctions held: ",auctions_total
print " "
print "Bids per items: " , printList(s_bids_per_item)
print "Min Max and Average price per item:"
print " " 
print printList(s_min_max_avg_price_per_item)
print "Min Max and Average no. of bids per item:"
print " "
print printList(s_min_max_avg_bids_per_item)
print " showing 20 bids with price higher than 100"
print " "
print func_printBids(s_price_high,20)
print "*********************************************************************************"

#persist the dataframe ina parquet file
Auction.select('*').write.save("hdfs://localhost:8020/user/cloudera/hw6/ebay_parquet",format="parquet",mode= "overwrite")


