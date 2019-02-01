#!/usr/bin/python3.5
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from cassandra.cluster import Cluster
import time

conf = SparkConf().set('spark.cassandra.connection.host', 'ec2-18-232-2-76.compute-1.amazonaws.com')
spark = SparkSession \
    .builder           \
    .config(conf=conf) \
    .appName("Real time prediction") \
    .master('spark://ip-10-0-0-14.ec2.internal:7077') \
    .getOrCreate()
sc = spark.sparkContext

#df = spark.read.format('org.apache.spark.sql.cassandra')  \
#               .options(table='de', keyspace='test')   \
#               .load().show()

# To append, primary key can't be duplicated. If a row has the same primary key, that row will be overwritten in Cassandra
df.write \
  .format('org.apache.spark.sql.cassandra') \
  .mode('append') \
  .options(table='de', keyspace='test') \
  .save()
