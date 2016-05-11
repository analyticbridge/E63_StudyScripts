# *********************************************
# Course: CSCI E63 - Big Dat Analytics
# For Homework Assignment 08
# Problem 02 - Python Kafka Consumer Client program 
# Student: Dongyow Lin
# Revision DAte: 3/31/2016
# **********************************************

# Import needed classes
from kafka import KafkaConsumer


# Create a Kafka Consumer with Kafka server and topic configuration
consumer = KafkaConsumer('spark_topic',
                         group_id='HW08-group',
                         bootstrap_servers=['localhost:9092'])

# Open a file for output
fout = open('hw08p2_consumer.log', 'w')

print 'Kakka Consumer started....'

# Receive all messages from Kafka queue stored in Consumer and process them
for message in consumer:
    # Print to console to indicate status
    print ("%s" % (message.value))
    
    # Also print to the output file for record
    fout.write(message.value + '\n')


fout.flush()
fout.close()
consumer.close()

print 'Kafka Consumer closed'
