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

#spark = SparkSession.builder.appName("Real time prediction").getOrCreate()
spark = SparkSession \
    .builder \
    .appName("Real time prediction") \
    .config(conf=SparkConf()) \
    .getOrCreate()
sc = spark.sparkContext


file = 'cyber_attack_subset.csv'
kafka_topic = 'cyber'
feature_list = pd.read_csv(file, nrows=1, header=None).values.tolist()[0]
feature_list.remove('Label')

#rfc_model = RandomForestClassificationModel.load('s3n://cyber-insight/rfc_model')
