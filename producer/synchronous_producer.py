from confluent_kafka import Producer
import socket
import json

conf = {'bootstrap.servers': "localhost:29092,localhost:29093",
        'acks': 'all',
        'client.id': socket.gethostname()
        }

if __name__ == '__main__':
    topic_name = 'test3'
    delivered_records = 0
    
    producer = Producer(conf)
    
    for e in range(50):
        data = {'number': e}
        record_value = json.dumps(data)
        print("Producing record: {}\t{}".format(e, record_value))
        producer.produce(topic_name, key=str(e), value=record_value)
        delivered_records = delivered_records + 1
        #flush() should be called prior to shutting down the producer to ensure all outstanding/queued/in-flight messages are delivered.
        producer.flush()
    print("{} messages were produced to topic {}".format(delivered_records, topic_name))