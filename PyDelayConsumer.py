from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
import time
import threading

consumer = KafkaConsumer(bootstrap_servers='localhost:9092',auto_offset_reset='earliest')
consumer.subscribe(['my-topic'])
def mytimer():
	threading.Timer(5.0, mytimer).start()
	time.sleep(30) # delays for 30 seconds
for message in consumer:
            print (message)
mytimer()
