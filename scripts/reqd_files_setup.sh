#!/usr/bin/env bash

#Copying spark jars
cd /usr/lib/spark/jars

#Copying only the jar files from s3 bucket
sudo aws s3 cp s3://$1/ . --recursive --exclude "*" --include "*.jar"

# #Running hive ql to create the schemas required
# cd /home/hadoop/
# aws s3 cp s3://$1/hive-schema.hql .
# hive -v -f hive-schema.hql
