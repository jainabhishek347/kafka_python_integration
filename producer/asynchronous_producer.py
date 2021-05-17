from confluent_kafka import Producer
import socket
import json
conf = {#'bootstrap.servers': "localhost:29092,localhost:29093",
        'bootstrap.servers': "192.168.56.100:9092",
		'acks': 'all',
        'client.id': socket.gethostname()
        }

if __name__ == '__main__':
    topic_name = 'test3'
    delivered_records = 0
    
    producer = Producer(conf)
    
    # Optional per-message on_delivery handler (triggered by poll() or flush())
    # when a message has been successfully delivered or
    # permanently failed delivery (after retries).
    def producer_callback(err, msg):
        global delivered_records
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1
            print("Produced record to topic {} partition [{}] @ offset {}, key as {} and value {}"
                  .format(msg.topic(), msg.partition(), msg.offset(),msg.key(), msg.value()))
    
    for e in range(50):
        data = {'number' : e}
        record_value = json.dumps(data)
        print(record_value)
        producer.produce(topic_name, key=str(e), value=record_value, on_delivery=producer_callback)
        # p.poll() serves delivery reports (on_delivery)
        # from previous produce() calls.
        producer.poll(0)
    
    producer.flush()
    print("{} messages were produced to topic {}".format(delivered_records, topic_name))