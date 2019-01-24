#!/usr/bin/python3.5
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from kafka import KafkaConsumer
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql.types import StringType
import numpy as np
from json import loads
import pandas as pd
import time


# Create spark session
spark = SparkSession \
    .builder \
    .appName("Real time prediction") \
    .config(conf=SparkConf()) \
    .getOrCreate()

# Get proper feature names
file = 'cyber_attack_subset.csv'
kafka_topic = 'cyber'
feature_list = pd.read_csv(file, nrows=1, header=None).values.tolist()[0]
feature_list.remove('Label')

# Reload trained randomforest model from s3
rfc_model = RandomForestClassificationModel.load('s3n://cyber-insight/rfc_model')

# Initiate a consumer using kafka-python module
consumer = KafkaConsumer(
    'cyber',
     bootstrap_servers=['ec2-54-80-57-187.compute-1.amazonaws.com:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

# Read the message from producer and tranformed the data into a DataFrame
# Then get the feature vector. Predict using the trained model
for message in consumer:
    message_dict = message.value
    del message_dict[u'Label']
    message_dict = {str(k):float(v) for k, v in message_dict.items()}
    df = pd.DataFrame(message_dict, index=range(1))
    df = df[feature_list]
    df = spark.createDataFrame(df)
    df.na.fill(0)
    assembler_feats=VectorAssembler(inputCols=feature_list, outputCol='features')
    feat_data = assembler_feats.transform(df)
    predict = rfc_model.transform(feat_data)
    results = predict.select(['probability', 'label']).collect()
    results.show()
