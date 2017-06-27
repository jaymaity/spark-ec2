#!/bin/bash

/root/spark-ec2/copy-dir /root/task
/root/spark/bin/spark-submit --master "spark://$MASTERS:7077" /root/task/test_sample_spark.py