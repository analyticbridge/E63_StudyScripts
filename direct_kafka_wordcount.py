

"""
 Counts words in UTF8 encoded, '\n' delimited text directly received from Kafka in every 2 seconds.
 Usage: direct_kafka_wordcount.py <broker_list> <topic>

 To run this on your local machine, you need to setup Kafka and create a producer first, see
 http://kafka.apache.org/documentation.html#quickstart

 and then run the example
    `$ bin/spark-submit --jars \
      external/kafka-assembly/target/scala-*/spark-streaming-kafka-assembly-*.jar \
      examples/src/main/python/streaming/direct_kafka_wordcount.py \
      localhost:9092 test`

  Start zookeeper:  ./zookeeper-server-start.sh /home/cloudera/Downloads/kafka_2.11-0.8.2.2/config/zookeeper.properties 

  and then Kafka:  ./kafka-server-start.sh /home/cloudera/Downloads/kafka_2.11-0.8.2.2/config/server.properties 
      
"""

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: direct_kafka_wordcount.py <broker_list> <topic>")
        exit(-1)

    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 1) 
    ssc.checkpoint('/home/cloudera/Downloads/spark-stream-checkpoint') # Sets the checkpoint directory
    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    lines = kvs.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 30, 5)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
