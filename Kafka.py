from kafka import KafkaConsumer, KafkaProducer
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time
from kafka import KafkaProducer
import json

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Send message to Kafka topic
def send_message(topic, message):
        print(message)
        producer.send(topic, json.dumps(message).encode('utf-8'))


# Example usage
#send_message('selenium_tests', {'test_id': 123, 'action': 'start_test'})

producer.close()

