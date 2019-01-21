#!/usr/bin/env python3
from kafka import KafkaProducer
import time
import json
import smart_open
import config

server_address = 'server address'
producer = KafkaProducer(bootstrap_servers=server_address)
csv_stream = smart_open.smart_open('https://s3.amazonaws.com/cyber-insight/all_cyber_attack_data.csv')
