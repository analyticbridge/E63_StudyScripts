from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime

consumer = KafkaConsumer(bootstrap_servers='localhost:9092',auto_offset_reset='earliest')
consumer.subscribe(['test'])
print("After connecting to kafka")
for message in consumer:
	print(message)
