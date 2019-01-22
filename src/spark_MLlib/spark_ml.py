from pyspark import SparkContext
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
<<<<<<< HEAD
from pyspark.sql.functions import col
from pyspark.sql.types import *
=======
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
>>>>>>> 949241711c667109d140e87356e7f3db53218741
from pyspark.ml import Pipeline
import numpy as np


data = 'https://s3.amazonaws.com/cyber-insight/cyber_attack_subset.csv'
sc = SparkContext("public DNS here", "Build Attack Classification Model")
rdd = sc.textFile(data)
df = spark.read.csv(rdd)

<<<<<<< HEAD
#udf_to_category = udf(binarize, StringType())
#df = df.withColumn('binary_event', udf_to_category('Label'))

feat_cols = df.columns
feat_cols.remove('Label')
=======
udf_to_category = udf(binarize, StringType())
df = df.withColumn('binary_event', udf_to_category('Label'))

feat_cols = df.columns.tolist().remove('Label')
feat_cols = feat_cols.remove('binary_event')
>>>>>>> 949241711c667109d140e87356e7f3db53218741

assembler_feats=VectorAssembler(inputCols=feat_cols, outputCol='features')
label_indexer = StringIndexer(inputCol='Label', outputCol="label")
pipeline = Pipeline(stages=[assembler_feats, label_indexer])

all_data = pipeline.fit(df).transform(df)
all_data.cache()
data_train, data_test = all_data.randomSplit([0.75, 0.25], seed=123)

rfc = RandomForestClassifier(labelCol='label', featuresCol='features', numTrees=100)
trained_model = rfc.fit(data_train)
predict = trained_model.transform(data_test)

results = predict.select(['probability', 'label']).collect()
results_list = [(float(i[0][0]), 1.0-float(i[1])) for i in results]
score_and_labels = sc.parallelize(results_list)
metrics=BinaryClassificationMetrics(score_and_labels)
print('ROC score is: ', metrics.areaUnderROC)
trained_model.write().overwrite().save('s3://cyber-insight/')
