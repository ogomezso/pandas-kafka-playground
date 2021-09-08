from confluent_kafka import DeserializingConsumer
from confluent_kafka.error import ValueDeserializationError

total_count = 0


class KafkaAvroConsumer(object):
    def __init__(self, consumer_conf):
        self.consumer_conf = consumer_conf
        self.consumer = DeserializingConsumer(consumer_conf)

    def subscribe(self, topic):
        self.consumer.subscribe(topic)

    def consume(self, topic):
        global total_count
        self.subscribe(topic)
        while True:
            try:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    # No message available within timeout.
                    # Initial message consumption may take up to
                    # `session.timeout.ms` for the consumer group to
                    # rebalance and start consuming
                    print("Waiting for message or event/error in poll()")
                    continue
                elif msg.error():
                    print('error: {}'.format(msg.error()))
                else:
                    key_object = msg.key()
                    value_object = msg.value()
                    total_count += 1
                    print("Consumed record with key {} and value {}, and updated total count to {}"
                          .format(key_object.to_json(), value_object.to_json(), total_count))
            except KeyboardInterrupt:
                break
            except ValueDeserializationError as e:
                # Report malformed record, discard results, continue polling
                print("Message deserialization failed {}".format(e))
                pass

        self.consumer.close()

    def close(self):
        self.consumer.close()
