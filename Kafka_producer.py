import time
from random import randint
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')
while True:
	number = randint(0, 10)
	print('Sending: ' + str(number))
	producer.send('cs63Kafka_topic', str(number))
	time.sleep(1)