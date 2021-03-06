#!/usr/bin/python3.5
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler, IndexToString
from pyspark.ml.classification import RandomForestClassifier
from pyspark.mllib.evaluation import BinaryClassificationMetrics as metric
from pyspark.ml.evaluation import MulticlassClassificationEvaluator as multi_metric
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from sklearn.metrics import confusion_matrix
import os, boto
from tools import utility

def rfc_train(df):
    """
    This is the main function to train a random
    forest classifier on the dataframe read from
    the csv file on s3
    """

    assembler_feats=VectorAssembler(inputCols=feat_cols, outputCol='features')
    label_indexer = StringIndexer(inputCol='Label', outputCol="target")
    pipeline = Pipeline(stages=[assembler_feats, label_indexer])

    # Transform the data and do a random split
    all_data = pipeline.fit(df).transform(df)
    all_data.cache()
    data_train, data_test = all_data.randomSplit([0.75, 0.25], seed=123)
    # Get class labels for using IndexToString to convert back from indexes
    Labels = all_data.groupBy('Label').count().orderBy('count', ascending=False).select('Label').collect()
    Labels = [row.Label for row in Labels]
    # Convert label indexes back to actual labels
    converter = IndexToString(inputCol='prediction', outputCol='predicted_label', labels=Labels)

    # Initiate a RandomForest model and train on the data
    rfc = RandomForestClassifier(labelCol='target', featuresCol='features', numTrees=100)
    trained_model = rfc.fit(data_train)
    predict = trained_model.transform(data_test)
    results = converter.transform(predict)

    converted = converter.transform(predict)
    # Calculate the combined accuracy to evaluate the model performance
    evaluator = multi_metric(labelCol='target', predictionCol='prediction', metricName='accuracy')
    print('Accuracy is: ', evaluator.evaluate(results))
    trained_model.write().overwrite().save('s3n://cyber-insight/rfc_model_multi')

if __name__ == '__main__':
    spark = SparkSession.builder.appName('Build Attack Classification Model') \
                                .master('spark://ip-10-0-0-14.ec2.internal:7077') \
                                .getOrCreate()
    sc = spark.sparkContext
    bk = utility.conn_s3()
    path = 's3n://cyber-insight/cyber_attack_multi.csv'
    df = spark.read.csv(path, header = True, inferSchema = True)

    # Get feature columns
    feat_cols =    ['Bwd Pkt Len Min',
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
    df = convertColumn(df, feat_cols, FloatType())
    rfc_train(df)
