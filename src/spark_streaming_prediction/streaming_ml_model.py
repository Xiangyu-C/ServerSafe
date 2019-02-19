#!/usr/bin/python3.5
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark.streaming import StreamingContext
from pyspark.ml.feature import VectorAssembler
from pyspark.streaming.kafka import KafkaUtils
from cassandra.cluster import Cluster
from cassandra.util import uuid_from_time
from datetime import datetime
from json import loads
import time
import os, boto


# Set up connection with S3
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')
conn = boto.connect_s3(aws_access_key, aws_secret_access_key)
bk = conn.get_bucket('cyber-insight', validate=False)

# Create spark session
conf = SparkConf().set('spark.cassandra.connection.host', 
                       'ec2-18-232-2-76.compute-1.amazonaws.com')
spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .appName("Real time prediction") \
    .master('spark://ip-10-0-0-14.ec2.internal:7077') \
    .getOrCreate()

sc = spark.sparkContext

# Connect to Cassandra DB for writing prediction results
cluster = Cluster(['ec2-18-232-2-76.compute-1.amazonaws.com'])
cass_session = cluster.connect('cyber_id')

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
                    'Label',
                    'Source',
                    'Destination',
                    'Timestamp'
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

# Reload trained randomforest model from s3 and instantiate VectorAssembler for transforming
rfc_model = RandomForestClassificationModel.load('s3n://cyber-insight/rfc_model_multi')
assembler_feats = VectorAssembler(inputCols=feature_list, outputCol='features')

def attacks_and_count_per_server(df, tl):
    """
    This function takes a dataframe returned by machine learning
    model and query it to find out two metrics:
    1. Attacks predicted per server per second
    2. Total traffic per server per second
    Then write to a Cassandra table
    """
    df.createOrReplaceTempView('results')
    # Get malicious predictions per server IP
    all_predictions = spark.sql("select Destination, count(*) as count from results \
                                 where prediction=1 group by Destination order by Destination")
    # Get total traffic per server IP
    all_traffic = spark.sql("Select Destination, count(*) as count from results \
                             group by Destination order by Destination")

    # Collect values for attack predictions and also total visits per second
    s1a, s2a, s3a, s4a, s5a, s6a, s7a, s8a, s9a, s10a, s11a, s12a, s13a = \
    all_predictions.collect()

    s1t, s2t, s3t, s4t, s5t, s6t, s7t, s8t, s9t, s10t, s11t, s12t, s13t = \
    all_traffic.collect()
    # Calculate per second stats
    s1a=s1a[1]/tl; s2a=s2a[1]/tl; s3a=s3a[1]/tl; s4a=s4a[1]/tl; s5a=s5a[1]/tl; s6a=s6a[1]/tl; s7a=s7a[1]/tl
    s8a=s8a[1]/tl; s9a=s9a[1]/tl; s10a=s10a[1]/tl; s11a=s11a[1]/tl; s12a=s12a[1]/tl; s13a=s13a[1]/tl

    s1t=s1t[1]/tl; s2t=s2t[1]/tl; s3t=s3t[1]/tl; s4t=s4t[1]/tl; s5t=s5t[1]/tl; s6t=s6t[1]/tl; s7t=s7t[1]/tl
    s8t=s8t[1]/tl; s9t=s9t[1]/tl; s10t=s10t[1]/tl; s11t=s11t[1]/tl; s12t=s12t[1]/tl; s13t=s13t[1]/tl


    # Insert attacks per second into cassandra table
    cass_session.execute(
    """
    insert into count_attack (id, t, a, b, c, d, e, f, g, h, i, j, k, l, m)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """,
    ('rate', uuid_from_time(datetime.now()), s1a, s2a, s3a, s4a, s5a, s6a,   \
               s7a, s8a, s9a, s10a, s11a, s12a, s13a)                        \
    )

    # Insert traffic per second into cassandra table
    cass_session.execute(
    """
    insert into count_traffic (id, t, a, b, c, d, e, f, g, h, i, j, k, l, m)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """,
    ('rate', uuid_from_time(datetime.now()), s1t, s2t, s3t, s4t, s5t, s6t,   \
               s7t, s8t, s9t, s10t, s11t, s12t, s13t)                        \
    )

def convertColumn(df, names, newType):
    """
    Convert all columns to float type
    """
    for name in names:
        df = df.withColumn(name, df[name].cast(newType))
    return(df)

def getSparkSessionInstance(sparkConf):
    '''
    Function to find spark session 
    '''
    conf = SparkConf().set('spark.cassandra.connection.host', 
                       'ec2-18-232-2-76.compute-1.amazonaws.com')
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=conf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

def predict_and_save(rdd):
    """
    This function processes the json dstream and convert
    to a dataframe for the ML model to predict on the 
    server traffic events and save to a cassandra table
    """
    spark = getSparkSessionInstance(rdd.context.getConf())
    df = spark.read.json(rdd)
    df = convertColumn(df, feature_list, FloatType())
    new_data = assembler_feats.transform(df)
    predict = rfc_model.transform(new_data)
    predictions = predict.select(['Timestamp', 'Label', 'prediction', 'Source', 'Destination'])
    # Save per server results into tables
    attacks_and_count_per_server(predictions, 3)
    # Now save the raw prediction results into another table
    predictions.write \
      .format('org.apache.spark.sql.cassandra') \
      .mode('append') \
      .options(table='all_predictions', keyspace='cyber_id') \
      .save()

# Get proper kafka topic name
kafka_topic = 'cyber'

# Create Dstream and start streaming
ssc = StreamingContext(sc, 3)
ssc.checkpoint('home/ubuntu/batch/cyber/')

kvs = KafkaUtils.createDirectStream(ssc, [kafka_topic],
                                    {'metadata.broker.list': 'ec2-54-80-57-187.compute-1.amazonaws.com:9092',
                                    'auto.offset.reset': 'smallest'})
# Load json messages
parsed_msg = kvs.map(lambda x: json.loads(x[1]))
# Process the messages
parsed_msg.foreachRDD(predict_and_save)

ssc.start()
ssc.awaitTermination()