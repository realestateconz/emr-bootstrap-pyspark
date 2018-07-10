#!/usr/bin/env bash

# ----------------------------------------------------------------------
#              Install Anaconda (Python 2) & Set To Default
# ----------------------------------------------------------------------
wget --quiet https://repo.continuum.io/archive/Anaconda2-5.2.0-Linux-x86_64.sh -O ~/anaconda2.sh
bash ~/anaconda2.sh -b -p $HOME/anaconda2
echo -e '\nexport SPARK_HOME=/usr/lib/spark\nexport PATH=$HOME/anaconda2/bin:$PATH' >> $HOME/.bashrc && source $HOME/.bashrc


#Dependencies for MySQL
sudo yum check-update -quiet
sudo yum -y install mysql mysql-server
sudo yum -y install mysql-connector-java


# install packages
conda install -y ipython jupyter
conda install -y anaconda nltk
conda install -y -c conda-forge fuzzywuzzy
conda install -y -c conda-forge python-levenshtein
conda install -y -c conda-forge pysal
conda install -y -c anaconda sqlalchemy
conda install -y -c anaconda pymysql
conda install -y mysqlclient

# cleanup:
#rm ~/anaconda2.sh

# enable https://github.com/mozilla/jupyter-spark:
sudo mkdir -p /usr/local/share/jupyter
sudo chmod -R 777 /usr/local/share/jupyter
conda install -y -c akode jupyter-spark
jupyter serverextension enable --py jupyter_spark
jupyter nbextension install --py jupyter_spark
jupyter nbextension enable --py jupyter_spark
jupyter nbextension enable --py widgetsnbextension

# cleanup:
rm ~/anaconda2.sh
