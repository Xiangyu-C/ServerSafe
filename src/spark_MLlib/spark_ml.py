#!/usr/bin/python3.5
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.mllib.evaluation import BinaryClassificationMetrics as metric
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
from pyspark.ml import Pipeline
import os, boto


# Setup spark and read in training data
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')
conn = boto.connect_s3(aws_access_key, aws_secret_access_key)
bk = conn.get_bucket('cyber-insight', validate=False)

spark = SparkSession.builder.appName('Build Attack Classification Model') \
                            .master('spark://ip-10-0-0-14.ec2.internal:7077') \
                            .getOrCreate()
sc = spark.sparkContext
path = 's3n://cyber-insight/cyber_attack_subset_new.csv'
df = spark.read.csv(path, header = True, inferSchema = True)

# Get feature columns
feat_cols = df.columns
feat_cols.remove('Label')

# Convert features to float type
def convertColumn(df, names, newType):
  for name in names:
     df = df.withColumn(name, df[name].cast(newType))
  return(df)

df = convertColumn(df, feat_cols, FloatType())

# Build a pipeline to transform the data into feature vectors
assembler_feats=VectorAssembler(inputCols=feat_cols, outputCol='features')
label_indexer = StringIndexer(inputCol='Label', outputCol="target")
pipeline = Pipeline(stages=[assembler_feats, label_indexer])

# Transform the data and do a random split
all_data = pipeline.fit(df).transform(df)
all_data.cache()
data_train, data_test = all_data.randomSplit([0.75, 0.25], seed=123)

# Initiate a RandomForest model and train on the data
rfc = RandomForestClassifier(labelCol='target', featuresCol='features', numTrees=100)
trained_model = rfc.fit(data_train)
predict = trained_model.transform(data_test)

# Evaluate the model using test data and output the AUC score
# Save the trained model into s3 bucket
results = predict.select(['probability', 'target']).collect()
results2 = predict.select(['prediction', 'target']).collect()
results_list = [(float(i[0][0]), 1.0-float(i[1])) for i in results]
results_list2 = [(float(i[0][0]), 1.0-float(i[1])) for i in results]
score_and_labels = sc.parallelize(results_list)
metrics=metric(score_and_labels)
print('ROC score is: ', metrics.areaUnderROC)
trained_model.write().overwrite().save('s3n://cyber-insight/rfc_model_new')
