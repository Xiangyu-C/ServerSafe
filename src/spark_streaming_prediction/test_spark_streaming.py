from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

sc = SparkContext(appName="PythonSparkStreamingKafka")

sc.setLogLevel("WARN")

ssc = StreamingContext(sc,5)
kafkaStream = KafkaUtils.createStream(ssc,
                                     'ec2-54-80-57-187.compute-1.amazonaws.com:9092',
                                     'my-group',
                                     {'cyber':1}
                                     )

lines = kafkaStream.map(lambda x: x[1])
lines.pprint()
ssc.start()
ssc.awaitTermination()
ssc.stop()
