from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
from random import randint
import threading

producer = KafkaProducer(bootstrap_servers='localhost:9092')
def mytimer():
   threading.Timer(5.0, mytimer).start()
   producer.send("test",str(randint(0,9)))

mytimer()

