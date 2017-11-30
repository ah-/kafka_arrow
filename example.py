import kafka_arrow
from confluent_kafka import Consumer, TopicPartition

consumer = Consumer({'bootstrap.servers': 'localhost:9092', 'group.id': 'kafka_arrow_test'})
consumer.assign([TopicPartition('test', 0, 0)])

while True:
    batch, error = kafka_arrow.poll(consumer, max_messages=5)

    if batch:
        print(batch.to_pandas())

    print(repr(error))
