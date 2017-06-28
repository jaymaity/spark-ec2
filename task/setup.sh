#!/bin/bash
echo $(hostname -f)
echo $(pwd)

for node in $SLAVES; do
  echo $node
  ssh -t -t $SSH_OPTS root@$node "/root/spark-ec2/task/setup-slave.sh" & sleep 0.3
  echo $node
done
wait

for node in $MASTERS; do
  echo $node

  ssh -t -t $SSH_OPTS root@$node "/root/spark-ec2/task/setup-master.sh" & sleep 0.3
  echo $node
done
wait

