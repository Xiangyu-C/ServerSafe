#!/usr/bin/python3.5
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.sql import SparkSession
from kafka import KafkaConsumer
from pyspark.ml.feature import StringIndexer, VectorAssembler
from cassandra.cluster import Cluster
from json import loads
import time
import os, boto


# Set up connection with S3
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

sc = spark.sparkContext

# Connect to Cassandra DB for writing prediction results
cluster = Cluster(['ec2-18-232-2-76.compute-1.amazonaws.com'])
cass_session = cluster.connect('cyber_id')

# Get proper feature names
kafka_topic = 'cyber'

feature_list_all = ['Bwd Pkt Len Min',
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
                    'Pkt Size Avg',
                    'Label'
                    ]

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


# Read the message from producer and tranformed the data into a DataFrame
# Then get the feature vector. Predict using the trained model
n = 1  # First index in Cassandra table
start = time.time()
for message in consumer:
    # Note now all the keys are not the same order as we want
    message_dict = message.value
    df = spark.createDataFrame([message_dict])
    # Reorder all columns to match format of training data seen by model
    df = df.select(feature_list_all)
    assembler_feats = VectorAssembler(inputCols=feature_list, outputCol='features')
    new_data = assembler_feats.transform(df)
    predict = rfc_model.transform(new_data)
    predictions = predict.select(['Label', 'prediction']).collect()
    cass_session.execute(
        """
        insert into cyber_ml (id, true_label, prediction)
        values (%s, %s, %s)
        """,
        (n, predictions[0][0], predictions[0][1])
    )
    n+=1
    if n>=1000:
        end = time.time()
        print('prediction speed at ', 1000/(end-start), ' msgs/sec')
        break
