#!/bin/bash
mkdir /root/task
sudo yum -y update
sudo yum install -y awslogs
sudo rm -f /var/log/agent-state
sudo aws s3 cp s3://distrib-load-test-bucket/awslogs.conf /etc/awslogs/.
sudo aws s3 cp s3://distrib-load-test-bucket/awscli.conf /etc/awslogs/.