#!/bin/bash

sudo yum -y update
# run automated spark script
/root/spark/bin/spark-submit --master "spark://$MASTERS:7077" /root/task/test_sample_spark.py