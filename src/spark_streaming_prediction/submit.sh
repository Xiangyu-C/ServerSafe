#!/bin/bash
spark-submit --packages datastax:spark-cassandra-connector:2.3.1-s_2.11,org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 streaming_processing.py --deploy-mode cluster
