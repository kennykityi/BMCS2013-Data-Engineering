"""
--------------------------
Author: Kelly Tan Jie Li |
--------------------------

"""

from kafka import KafkaProducer
import time
from Stream_Process import stream_process

bootstrap_servers = "localhost:9092"
topic = 'article'
time_interval = 2

producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

for num in range(1, 1000):  
    message = stream_process.api_crawl(num).encode('utf-8')
    if message.decode('utf-8') == "---":
        break  
    print(message.decode('utf-8'))  
    producer.send(topic, message)
    time.sleep(time_interval)

producer.flush()
