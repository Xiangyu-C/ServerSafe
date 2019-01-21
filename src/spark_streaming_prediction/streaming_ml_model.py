from pyspark.ml.classification import RandomForestClassificationModel
from pyspark import SparkContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaConsumer
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.evaluation import BinaryClassificationMetrics
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import numpy as np
from json import loads

file = 'https://s3.amazonaws.com/cyber-insight/cyber_attack_subset.csv'
sc = SparkContext(appName='PythonStreamingPrediction')
kafka_topic = 'cyber_events'
feature_list = pd.read_csv(file2, nrows=1, header=None).values.tolist()[0]

rfc_model = RandomForestClassificationModel.load('s3://cyber-insight/trained_model')

consumer = KafkaConsumer(
    'cyber_events',
     bootstrap_servers=['EC2 nodes here'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

for message in consumer:
    df=spark.read.json(sc.parellelize(message))
    df=df[feature]
    assembler_feats=VectorAssembler(inputCols=feat_cols, outputCol='features')
    #label_indexer = StringIndexer(inputCol='binary_response', outputCol="label")

    feat_data = assembler_feats.fit(df).transform(df)
    all_data.cache()
    predict = rfc_model.transform(feat_data)

    results = predict.select(['probability', 'label']).collect()
    print(results)
