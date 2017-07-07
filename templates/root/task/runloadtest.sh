#!/bin/bash
sudo yum install -y awslogs
echo "Installation done"
sudo rm -f /var/log/agent-state
echo "starting to copy"
sudo aws s3 cp s3://distrib-load-test-bucket/testjay/awslogs.conf /etc/awslogs/.
sudo aws s3 cp s3://distrib-load-test-bucket/awscli.conf /etc/awslogs/.
sudo aws s3 cp s3://distrib-load-test-bucket/testjay/distrib-load-test-app.tar.gz ~/
echo "Print log done"
cd ~/
tar -zxvf distrib-load-test-app.tar.gz
cd distrib-load-test
sudo rm usersimulation.log
sudo rm stresstest.log
sudo service awslogs restart
export LD_LIBRARY_PATH=~/distrib-load-test/:$LD_LIBRARY_PATH
EC2_AVAIL_ZONE=`curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone`
EC2_REGION="`echo \"$EC2_AVAIL_ZONE\" | sed -e 's:\([0-9][0-9]*\)[a-z]*\$:\\1:'`"
./distrib-load-test-app $(hostname) $EC2_REGION
export SIM_LOG=$(hostname)_$(date +%m-%d-%yT%T)_usersimulation.log
export ST_LOG=$(hostname)_$(date +%m-%d-%yT%T)_stresstest.log
aws s3 cp usersimulation.log s3://distrib-load-test-bucket/output/$SIM_LOG --storage-class REDUCED_REDUNDANCY
aws s3 cp stresstest.log s3://distrib-load-test-bucket/output/$ST_LOG --storage-class REDUCED_REDUNDANCY

