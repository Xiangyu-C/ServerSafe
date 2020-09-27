#!/usr/bin/env python3
from kafka import KafkaProducer
import time
import json
from smart_open import smart_open
import pandas as pd
import boto3


def get_file(bucket_name):
    """
    This function fetch the csv file and get
    column names from preprocessed files
    """

    client = boto3.client('s3')
    resource = boto3.resource('s3')
    my_bucket = resource.Bucket(bucket_name)
    obj1 = client.get_object(Bucket=bucket_name, Key='cyber_attack_stream_data.csv')
    obj2 = client.get_object(Bucket=bucket_name, Key='cyber_attack_subset_new.csv')

    # Get column names for streaming data and for features used in ML model
    all_headers = pd.read_csv(obj1['Body'], nrows=1, header=None).values.tolist()[0]
    feature_list = pd.read_csv(obj2['Body'], nrows=1, header=None).values.tolist()[0]
    # Get generated IPs and trained_model
    feature_list.extend(['Source', 'Destination', 'Timestamp'])
    # Get indexes for features in headers in streaming data
    feature_index_list = [all_headers.index(x) for x in feature_list]

    return feature_list, feature_index_list

def get_producer(server_address):
    """
    This function takes a server address
    and return a kafka-python producer
    """
    producer = KafkaProducer(bootstrap_servers=server_address)
    return producer

def is_number(s):
    """
    This function checks if a data point is a number
    """
    if s.isdigit():
        return True
    else:
        return False

def convert_to_dict_then_json(row, sep, feature_list, feature_index_list):
    """
    This function will read each row from the csv file
    and serialize using json after validating the data
    """
    list_temp = row.decode('utf-8').replace('\n', '').replace('\r', '').split(sep)
    feature_values = [list_temp[i] for i in feature_index_list]
    time_stamp = feature_values.pop(-1)
    dest_ip = feature_values.pop(-1)
    source_ip = feature_values.pop(-1)
    label = feature_values.pop(-1)
    feature_values_clean = [float(x) if is_number(x) else 0 for x in feature_values]
    feature_values_clean.extend([label, source_ip, dest_ip, time_stamp])
    feat_dict = dict(zip(feature_list, feature_values_clean))
    feat_json = json.dumps(feat_dict).encode('utf-8')
    return feat_json

if __name__ == '__main__':
    server_address = 'ec2-54-80-57-187.compute-1.amazonaws.com:9092'
    producer = get_producer(server_address)
    # Instantiate the csv stream using smart open
    csv_stream = smart_open('s3://cyber-insight/cyber_attack_stream_new.csv')
    bucket_name = 'cyber-insight'
    kafka_topic = 'cyber'
    sep = ','

    feature_list, feature_index_list = get_file(bucket_name)
    n = 0
    start = time.time()
    for line in csv_stream:
        # Skip header row
        if n != 0:
            producer.send(kafka_topic, convert_to_dict_then_json(line,
                                                                 sep,
                                                                 feature_list,
                                                                 feature_index_list))
        n += 1
        # Stream 10 million rows and then stop and print producer speed
        if n >= 10000000:
            end = time.time()
            print(f'End of streaming. Sent at {10000000/(end-start)} msg/sec')
            break
