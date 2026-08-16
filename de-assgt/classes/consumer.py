"""
--------------------------
Author: Kelly Tan Jie Li |
--------------------------

"""

from kafka import KafkaConsumer

bootstrap_servers = "localhost:9092"
topic = 'article'

consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers)

for msg in consumer:
    message = msg.value.decode('utf-8')
    print(message)


