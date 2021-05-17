
from confluent_kafka import Producer
import socket

conf = {#'bootstrap.servers': "localhost:29092,localhost:29093",
        'bootstrap.servers': "192.168.99.104:9092",

		'acks':'all',
        'client.id': socket.gethostname()
        }

topic_name = 'test3'
print(conf)
producer = Producer(conf)
def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg.value()), str(err)))
    else:
        print("Message produced: %s" % (str(msg.value())))
print("data")
for e in range(3):
    data = {'number' : e}
    print(producer)
    producer.produce(topic_name, key=str(e), value=str(e), callback=acked)
    producer.poll(1)

print("end")