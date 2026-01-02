from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(bootstrap_servers='kafka:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

for i in range(100):
    data = {'event_id': i, 'event_value': i*10, 'timestamp': time.time()}
    producer.send('my-topic', data)
    time.sleep(1)