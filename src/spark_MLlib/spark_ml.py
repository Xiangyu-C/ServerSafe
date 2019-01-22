from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.mllib.evaluation import BinaryClassificationMetrics as metric
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
from pyspark.ml import Pipeline


data = 'cyber_attack_subset.csv'
spark = SparkSession.builder.appName('Build Attack Classification Model').getOrCreate()
sc = sparkContext
df = spark.read.csv(data, header = True, inferSchema = True)

feat_cols = df.columns
feat_cols.remove('Label')

def convertColumn(df, names, newType):
  for name in names:
     df = df.withColumn(name, df[name].cast(newType))
  return(df)

df = convertColumn(df, feat_cols, FloatType())
#udf_to_category = udf(binarize, StringType())
#df = df.withColumn('binary_event', udf_to_category('Label'))

assembler_feats=VectorAssembler(inputCols=feat_cols, outputCol='features')
label_indexer = StringIndexer(inputCol='Label', outputCol="target")
pipeline = Pipeline(stages=[assembler_feats, label_indexer])

all_data = pipeline.fit(df).transform(df)
all_data.cache()
data_train, data_test = all_data.randomSplit([0.75, 0.25], seed=123)

rfc = RandomForestClassifier(labelCol='target', featuresCol='features', numTrees=100)
trained_model = rfc.fit(data_train)
predict = trained_model.transform(data_test)

results = predict.select(['probability', 'target']).collect()
results_list = [(float(i[0][0]), 1.0-float(i[1])) for i in results]
score_and_labels = sc.parallelize(results_list)
metrics=BinaryClassificationMetrics(score_and_labels)
print('ROC score is: ', metrics.areaUnderROC)
trained_model.write().overwrite().save('s3://cyber-insight/')
