#!/bin/bash

for node in $MASTERS; do
  echo $node
  ssh -t -t $SSH_OPTS root@$node "/root/task/setup-master.sh" & sleep 0.3
  echo $node

done
wait