#!/bin/sh

DATE=$(date +"%FT%T")
aws s3 cp $1 s3://$2/logs/$DATE --recursive --exclude "*" --include "*.log"
