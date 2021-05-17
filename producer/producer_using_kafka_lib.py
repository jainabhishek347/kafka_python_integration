from kafka import KafkaProducer
from time import sleep
from json import dumps
import json

topic_name = 'test3'
producer = KafkaProducer(bootstrap_servers='localhost:29092,localhost:29093',
    acks='all',
	value_serializer=lambda m: json.dumps(m).encode('ascii')
	)
for e in range(50):
    data = {'number' : e}
    print(data)
    producer.send(topic_name, value=data)    

producer.close()
