from kafka import KafkaConsumer

consumer = KafkaConsumer('cs63Kafka_topic')
while True:
	msg = next(consumer)
	print(msg)