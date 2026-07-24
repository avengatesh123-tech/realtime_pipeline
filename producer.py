import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

items = ['Mobile', 'Laptop', 'Watch']

while True:
    data = {"product_name": random.choice(items), "price": random.randint(100, 1000)}
    producer.send('order_topic', value=data)
    print(f" Sent: {data}")
    time.sleep(5) 
