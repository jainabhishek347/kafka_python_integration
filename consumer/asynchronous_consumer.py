import sys
import json

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

# The simplest and most reliable way to manually commit offsets is by setting the async parameter to the Consumer.commit() method call.

# Create Consumer instance
#'auto.offset.reset=earliest' to start reading from the beginning of the topic if no committed offsets exist
consumer_conf = {'bootstrap.servers': "localhost:29092",
        'group.id': "python_example_group_1",
        'auto.offset.reset': 'smallest'}

consumer = Consumer(consumer_conf)

topics = ['test3']
MIN_COMMIT_COUNT = 10
running= True

def consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        msg_count = 0
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
                total_count = +1
                print("Consumed record with key {} and value {} and topic {} and partition {} and offset {}, and updated total count to {}"
                      .format(record_key, data, topic_name, partition, offset, total_count))
                msg_count += 1
                # Commit a message or a list of offsets.
                # |
                # |      The ``message`` and ``offsets`` parameters are mutually exclusive. If neither is set, the current partition assignment's offsets are used instead. Use this method to commit offsets if you have 'enable.auto.commit' set to False.
                # |
                # |      :param confluent_kafka.Message message: Commit the message's offset+1. Note: By convention, committed offsets reflect the next message to be consumed, **not** the last message consumed.
                # |      :param list(TopicPartition) offsets: List of topic+partitions+offsets to commit.
                # |      :param bool asynchronous: If true, asynchronously commit, returning None immediately. If False, the commit() call will block until the commit succeeds or fails and the committed offsets will be returned (on success). Note that specific partitions may have failed and the .err field of each partition should be checked for success.
                # |      :rtype: None|list(TopicPartition)
                # |      :raises: KafkaException
                # |      :raises: RuntimeError if called on a closed consumer

                consumer.commit(msg, asynchronous=True)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

consume_loop(consumer, topics)