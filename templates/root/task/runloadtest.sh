#!/bin/sh
cd $1 
chmod 777 $1*.*
echo $(hostname -f)
$1distrib-load-test-app $(hostname -f) $2
            
