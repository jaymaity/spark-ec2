#!/bin/bash
sudo yum update
sudo /root/spark-ec2/copy-dir /root/task
sudo chmod 777 /root/task
sudo /root/spark/bin/spark-submit --master "spark://$MASTERS:7077" /root/task/test_sample_spark.py