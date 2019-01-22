from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
    'cyber',
     bootstrap_servers=['ec2-54-80-57-187.compute-1.amazonaws.com:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

for message in consumer:
    print(message)
