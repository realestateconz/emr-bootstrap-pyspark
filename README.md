# EMR Bootstrap PySpark with Anaconda

This code should help to jump start PySpark with Anaconda on AWS.

## Getting Started
1. `cd emr-bootstrap-pyspark` && `conda env create -f environment.yml`
2. Fill in all the required information e.g. aws access key, secret acess key etc. into the `config.yml.example` file and rename it to `config.yml`
3. Run it in the newly created environment - `source activate emr-bootstrap-pyspark` && `python emr_loader.py`
4. Make sure to have created the default roles in the AWS account by running `aws emr create-default-roles` in AWS CLI 

## Requirements
- [Anaconda 2](https://www.continuum.io/downloads)
- [AWS Account](https://aws.amazon.com/)
