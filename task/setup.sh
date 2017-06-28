#!/bin/bash
echo hostname
echo pwd
/root/spark-ec2/copy-dir /root/task

for node in $SLAVES; do
  echo $node
  ssh -t -t $SSH_OPTS root@$node "/root/task/setup-slave.sh" & sleep 0.3
  echo $node
done
wait

for node in $MASTERS; do
  echo $node

  ssh -t -t $SSH_OPTS root@$node "/root/task/setup-master.sh" & sleep 0.3
  echo $node
done
wait

