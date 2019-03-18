# Utility script to provide some commonly used functions

def conn_s3():
    """
    This function will build connection
    to S3 with credentials
    """

    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')
    conn = boto.connect_s3(aws_access_key, aws_secret_access_key)
    bk = conn.get_bucket('cyber-insight', validate=False)

    return(bk)

def spark_session():
    """
    This function will build spark cass_session
    and spark context with proper config
    """

    conf = SparkConf().set('spark.cassandra.connection.host',
                           'ec2-18-232-2-76.compute-1.amazonaws.com')   \
                      .set('spark.streaming.backpressure.enabled', True)  \
                      .set('spark.streaming.kafka.maxRatePerPartition', 5000)
    spark = SparkSession \
        .builder \
        .config(conf=conf)  \
        .appName("Real time prediction") \
        .master('spark://ip-10-0-0-14.ec2.internal:7077') \
        .getOrCreate()
    sc = spark.sparkContext

    return(spark, sc)

def cass_conn():
    """
    This function builds a connection
    to cassandra database
    """

    cluster = Cluster(['ec2-18-232-2-76.compute-1.amazonaws.com'])
    cass_session = cluster.connect('cyber_id')

    return(cass_session)

def convertColumn(df, names, newType):
    """
    This function converts all columns
    from a dataframe to float type
    """
    for name in names:
       df = df.withColumn(name, df[name].cast(newType))
    return(df)
