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

spark = SparkSession \
    .builder \
    .appName("Real time prediction") \
    .config("spark.executor.memory", "1gb") \
    .getOrCreate()

sc = spark.sparkContext
#sc = SparkContext("local", "MLapp")
file = 'cyber_attack_subset.csv'
kafka_topic = 'cyber'
feature_list = pd.read_csv(file, nrows=1, header=None).values.tolist()[0]
feature_list.remove('Label')

rfc_model = RandomForestClassificationModel.load('s3n://cyber-insight/rfc_model')

consumer = KafkaConsumer(
    'cyber',
     bootstrap_servers=['ec2-54-80-57-187.compute-1.amazonaws.com:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

for message in consumer:
    df=spark.read.json(sc.parallelize(message))
    df=df[feature_list]
    df.na.fill(0)
    assembler_feats=VectorAssembler(inputCols=feat_cols, outputCol='features')

    feat_data = assembler_feats.fit(df).transform(df)
    predict = rfc_model.transform(feat_data)

    results = predict.select(['probability', 'label']).collect()
    print(results)
    time.sleep(0.1)
