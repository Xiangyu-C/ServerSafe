#!/usr/bin/env python3
from kafka import KafkaProducer
import time
import json
import smart_open
import config
import pandas as pd

def main():
    server_address = 'server address'
    file1 = 'https://s3.amazonaws.com/cyber-insight/all_cyber_attack_data.csv'
    file2 = 'https://s3.amazonaws.com/cyber-insight/cyber_attack_subset.csv'
    producer = KafkaProducer(bootstrap_servers=server_address)
    csv_stream = smart_open.smart_open(file1)
    all_headers = pd.read_csv(file1, nrows=1, header=None).values.tolist()[0]
    feature_list = pd.read_csv(file2, nrows=1, header=None).values.tolist()[0]
    feature_index_list = [all_headers.index(x) for x in feature_list]
    kafka_topic = 'cyber_events'
    sep=','

    def convert_to_dict_then_json(row):
        list_temp = row.decode('utf-8').replace('\n', '').split(sep)
        feature_values = pd.Series(list_temp)[feature_index_list].tolist()
        feat_dict = dict(zip(feature_list, feature_values))
        feat_json = json.dumps(feat_dict).decode('utf-8')
        return(feat_json)

    n=0
    for line in csv_stream:
        if n!=0:
            producer.send(kafka_topic, convert_to_dict_then_json(line))
        n+=1
        if n>=10000:
            print('End of streaming')
            break

if __name__ == '__main__':
    main()
