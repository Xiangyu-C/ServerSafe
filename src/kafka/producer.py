#!/usr/bin/env python3
from kafka import KafkaProducer
import time
import json
from smart_open import smart_open
import pandas as pd
import boto3


# Define producer
def main():
    """
    Make a producer to send lines of a csv file to simulate data
    streaming into Spark
    """
    server_address = 'ec2-54-80-57-187.compute-1.amazonaws.com:9092'
    client = boto3.client('s3')
    resource = boto3.resource('s3')
    bucket_name = 'cyber-insight'
    my_bucket = resource.Bucket(bucket_name)
    obj1 = client.get_object(Bucket=bucket_name, Key='all_cyber_attack_data.csv')
    obj2 = client.get_object(Bucket=bucket_name, Key='cyber_attack_subset.csv')

    all_headers = pd.read_csv(obj1['Body'], nrows=1, header=None).values.tolist()[0]
    feature_list = pd.read_csv(obj2['Body'], nrows=1, header=None).values.tolist()[0]
    feature_index_list = [all_headers.index(x) for x in feature_list]

    # Initiate a producer using kafka-python and smart-open modules
    producer = KafkaProducer(bootstrap_servers=server_address)
    csv_stream = smart_open('s3://cyber-insight/cyber_attack_stream_data_part1.csv')

    kafka_topic = 'cyber'
    sep=','

    # Deserialize the stream and then convert to json then serialize to send it
    def convert_to_dict_then_json(row):
        list_temp = row.decode('utf-8').replace('\n', '').replace('\r', '').split(sep)
        feature_values = pd.Series(list_temp)[feature_index_list].tolist()
        feat_dict = dict(zip(feature_list, feature_values))
        feat_json = json.dumps(feat_dict).encode('utf-8')
        return(feat_json)

    # Send the lines to kafka at 1 record /sec for testing
    n=0
    for line in csv_stream:
        if n!=0:
            producer.send(kafka_topic, convert_to_dict_then_json(line))
        n+=1
        if n>=10000:
            print('End of streaming')
            break
        time.sleep(1)

if __name__ == '__main__':
    main()
