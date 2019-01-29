#!/usr/bin/python3.5
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from kafka import KafkaConsumer
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql.types import *
from pyspark.ml import Pipeline
from cassandra.cluster import Cluster
from json import loads
import pandas as pd
import time
import os, boto


# Establish connection with s3
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')
conn = boto.connect_s3(aws_access_key, aws_secret_access_key)
bk = conn.get_bucket('cyber-insight', validate=False)

# Create spark session
spark = SparkSession \
    .builder \
    .appName("Real time prediction") \
    .master('spark://ip-10-0-0-14.ec2.internal:7077') \
    .getOrCreate()
#spark.conf.set('spark.executor.memory', '5g')
#spark.conf.set('spark.executor.cores', 5)
#spark.conf.set('spark.cores.max', 5)
#spark.conf.set('spark.driver.memory', '5g')
sc = spark.sparkContext

#connect to Cassandra DB for writing prediction results
cluster = Cluster(['ec2-18-232-2-76.compute-1.amazonaws.com'])
cass_session = cluster.connect('cyber_id')

# Get proper feature names
kafka_topic = 'cyber'
feature_list = ['Bwd Pkt Len Min',
                'Subflow Fwd Byts',
                'TotLen Fwd Pkts',
                'Fwd Pkt Len Mean',
                'Bwd Pkt Len Std',
                'Flow IAT Mean',
                'Fwd IAT Min',
                'Flow Duration',
                'Flow IAT Std',
                'Active Min',
                'Active Mean',
                'Bwd IAT Mean',
                'Fwd IAT Mean',
                'Init Fwd Win Byts',
                'Fwd PSH Flags',
                'SYN Flag Cnt',
                'Fwd Pkts/s',
                'Init Bwd Win Byts',
                'Bwd Pkts/s',
                'PSH Flag Cnt',
                'Pkt Size Avg'
                ]

# Reload trained randomforest model from s3
rfc_model = RandomForestClassificationModel.load('s3n://cyber-insight/rfc_model_new')

# Initiate a consumer using kafka-python module
consumer = KafkaConsumer(
    'cyber',
     bootstrap_servers=['ec2-54-80-57-187.compute-1.amazonaws.com:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

def convertColumn(df, names, newType):
  for name in names:
     df = df.withColumn(name, df[name].cast(newType))
  return(df)


# Read the message from producer and tranformed the data into a DataFrame
# Then get the feature vector. Predict using the trained model
n = 1  # First index in Cassandra table
for message in consumer:
    message_dict = message.value
    #message_dict[u'Label']
    message_dict = {str(k):str(v) for k, v in message_dict.items()}
    df = pd.DataFrame(message_dict, index=range(1))
    #df = df[feature_list]
    df[feature_list] = df[feature_list].apply(pd.to_numeric, errors='coerce').fillna(0)
    df = spark.createDataFrame(df)
    df = convertColumn(df, feature_list, FloatType())
    assembler_feats = VectorAssembler(inputCols=feature_list, outputCol='features')
    #label_indexer = StringIndexer(inputCol='Label', outputCol="target")
    pipeline = Pipeline(stages=[assembler_feats, label_indexer])
    new_data = pipeline.fit(df).transform(df)
    predict = rfc_model.transform(new_data)
    #predictions = predict.select(['Label', 'prediction'])
    cass_session.execute(
        """
        insert into cyber_prediction (id, prediction, true_label)
        values (%s, %s, %s)
        """,
        (n, predict.select('prediction').first()['prediction'], predict.select('Label').first()['Label'])
    )
    n+=1
