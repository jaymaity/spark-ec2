#!/bin/bash

/root/spark-ec2/copy-dir /root/task
cd /root/task
python -m SimpleHTTPServer 8000

