import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import time
import threading
sc = SparkContext(appName="PyWordCount")
ssc = StreamingContext(sc, 2)
brokers,topic = "localhost:9092","test"
kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
lines = kvs.map(lambda x: x[1])
counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)
#counts.pprint()
#
def mytimer():
		threading.Timer(5.0, mytimer).start()
#		time.sleep(30)
		counts.pprint()
mytimer()

ssc.start()
ssc.awaitTermination()
