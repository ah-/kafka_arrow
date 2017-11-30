import kafka_arrow
from confluent_kafka import Consumer, TopicPartition

consumer = Consumer({'bootstrap.servers': 'localhost:9092', 'group.id': 'kafka_arrow_test'})
consumer.assign([TopicPartition('test', 0, 0)])

batch = kafka_arrow.poll(consumer, max_messages=10)

print(batch)
print(batch.schema)
print(batch.to_pandas())
