import sys
import json

from confluent_kafka import Consumer, KafkaError, KafkaException

# Create Consumer instance
#'auto.offset.reset=earliest' to start reading from the beginning of the topic if no committed offsets exist
consumer_conf = {#'bootstrap.servers': "localhost:29092",
        'bootstrap.servers': "192.168.99.104:9092",
        'group.id': "python_example_group_1",
        'auto.offset.reset': 'smallest'}

consumer = Consumer(consumer_conf)

topics = ['test3']

running= True

def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)
        total_count = 0
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                continue
            if msg.error():
                print(msg.error())
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                record_key = msg.key()
                record_value = msg.value()
                data = json.loads(record_value)
                topic_name = msg.topic()
                partition = msg.partition()
                offset = msg.offset()
                total_count =+1
                print("Consumed record with key {} and value {} and topic {} and partition {} and offset {}, \
                                      and updated total count to {}"
                      .format(record_key, data, topic_name, partition, offset, total_count))
                
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

basic_consume_loop(consumer, topics)