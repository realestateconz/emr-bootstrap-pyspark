# EMR Bootstrap
Originally repo details in this - https://medium.com/@datitran/quickstart-pyspark-with-anaconda-on-aws-660252b88c9a

# This code should help to
1. Bind PySpark with Anaconda2 on AWS.
2. Setup Zeppelin and Jupyter to use Anaconda2 on AWS.
3. Setup S3 backed zeppelin notebooks.
4. Setup Hive metastore to MySQL RDS instance.
5. Install required python libraries in all the nodes of the cluster.

## Getting Started
1. `cd emr-bootstrap-pyspark` && `conda env create -f environment.yml`
2. Fill in all the required information e.g. aws access key, secret acess key etc. into the `config.yml.example` file and rename it to `config.yml`
3. Run it in the newly created environment - `source activate emr-bootstrap-pyspark` && `python emr_loader.py`
4. Make sure to have created the default roles in the AWS account by running `aws emr create-default-roles` in AWS CLI

## Requirements
- [Anaconda 2](https://www.continuum.io/downloads)
- [AWS Account](https://aws.amazon.com/)
