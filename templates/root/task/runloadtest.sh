#!/bin/sh
sudo su
cd $1
export LD_LIBRARY_PATH="/root/task/distrib-load-test-app/":
chmod 777 $1*.*
echo $(hostname -f)
$1distrib-load-test-app $(hostname -f) $2
            
