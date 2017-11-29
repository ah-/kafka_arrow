import confluent_kafka
import kafka_arrow

consumer = confluent_kafka.Consumer({'bootstrap.servers': 'localhost:9092', 'group.id': 'kafka_arrow_test'})
consumer.assign([confluent_kafka.TopicPartition('test', 0, 0)])

batch = kafka_arrow.poll(consumer, max_messages=10)

print(batch)
print(batch.schema)
print(batch.to_pandas())
