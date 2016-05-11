
# *********************************************
# Course: CSCI E63 - Big Dat Analytics
# For Homework Assignment 08
# Problem 02 - Python Kafka Produce Client program 
# Student: Dongyow Lin
# Revision DAte: 3/31/2016
# **********************************************

# Import needed classes
from kafka import KafkaProducer
from random import randint
import time

# Create a Kafka Producer with Kafka server configurattion
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Open a file for output
fout = open('hw08p2_producer.log', 'w')

print 'Kafka Producer started....'

# Allow this to run forever until user interruption with Ctrl-C
while True:   
    # obtain a random number from 0 to 10 
    i = randint(0, 10)
    # Print to console to indicate status
    print 'sending message: value (' + str(i) + ')'
    
    # Send the text message to Kafka queue
    producer.send('spark_topic', b'' + str(i))
   
    # Also print to the output file for record
    fout.write(str(i) + '\n')
    
    # Sleeping for 1 seconds 
    time.sleep(1)


fout.flush()
fout.close()
producer.close()

print 'Kafka Producer done sending messages and closed.'
