#!/usr/bin/env bash

# bind conda to spark
echo -e "\nexport PYSPARK_PYTHON=/home/hadoop/anaconda2/bin/python" >> /etc/spark/conf/spark-env.sh
echo "export PYSPARK_DRIVER_PYTHON=/home/hadoop/anaconda2/bin/jupyter" >> /etc/spark/conf/spark-env.sh
echo "export PYSPARK_DRIVER_PYTHON_OPTS='notebook --no-browser'" >> /etc/spark/conf/spark-env.sh

# Restart Zeppelin
sudo stop zeppelin
sudo start zeppelin
