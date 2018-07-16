#!/usr/bin/env bash


#Copying config files
#Zeppelin config file
sudo aws s3 cp s3://$1/zeppelin-site.xml /etc/zeppelin/conf/

#gbq access
sudo aws s3 cp s3://$1/google_api_credentials.json /home/hadoop/

#Copying spark jars
cd /usr/lib/spark/jars

#Copying only the jar files from s3 bucket
sudo aws s3 cp s3://$2/ . --recursive --exclude "*" --include "*.jar"
