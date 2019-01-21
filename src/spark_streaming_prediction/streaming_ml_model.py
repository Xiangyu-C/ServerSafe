from pyspark.ml.classification import RandomForestClassificationModel
from pyspark import SparkContext
from pyspark.streaming.kafka import KafkaUtils

sc = SparkContext(appName='PythonStreamingPrediction')

rfc_model = RandomForestClassificationModel.load('s3://cyber-insight/trained_model')
