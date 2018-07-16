import boto3
import botocore
import yaml
import time
import logging
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class EMRLoader(object):
    def __init__(self, aws_access_key, aws_secret_access_key, region_name,
                 cluster_name, instance_count, master_instance_type, slave_instance_type,
                 key_name, subnet_id, log_uri, software_version, script_bucket_name, config_bucket_name, db_username, db_password):
        self.aws_access_key = aws_access_key
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.cluster_name = cluster_name
        self.instance_count = instance_count
        self.master_instance_type = master_instance_type
        self.slave_instance_type = slave_instance_type
        self.key_name = key_name
        self.subnet_id = subnet_id
        self.log_uri = log_uri
        self.software_version = software_version
        self.script_bucket_name = script_bucket_name
        self.config_bucket_name = config_bucket_name
        self.db_username = db_username
        self.db_password = db_password

    def boto_client(self, service):
        client = boto3.client(service,
                              aws_access_key_id=self.aws_access_key,
                              aws_secret_access_key=self.aws_secret_access_key,
                              region_name=self.region_name)
        return client

    def load_cluster(self):
        response = self.boto_client("emr").run_job_flow(
            Name=self.cluster_name,
            LogUri=self.log_uri,
            ReleaseLabel=self.software_version,
            Instances={
                'MasterInstanceType': self.master_instance_type,
                'SlaveInstanceType': self.slave_instance_type,
                'InstanceCount': self.instance_count,
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2KeyName': self.key_name,
                'Ec2SubnetId': self.subnet_id
            },
            Applications=[
                {'Name':'Hadoop'},
                {'Name':'Spark'},
                {'Name':'Ganglia'},
                {'Name':'Hive'},
                {'Name':'Hue'},
                {'Name':'Presto'},
                {'Name':'Zeppelin'},
                {'Name':'Oozie'}
            ],
            Configurations=[
                {
                "Classification": "hive-site",
                    "Properties": {
                        "javax.jdo.option.ConnectionURL": "jdbc:mysql://datascience-mysql.ds.readm.co.nz:3306/hive?createDatabaseIfNotExist=true",
                        "javax.jdo.option.ConnectionDriverName": "org.mariadb.jdbc.Driver",
                        "javax.jdo.option.ConnectionUserName": self.db_username,
                        "javax.jdo.option.ConnectionPassword": self.db_password
                        }
                }
            ],
            BootstrapActions=[
                {
                    'Name': 'Install Anaconda2',
                    'ScriptBootstrapAction': {
                        'Path': 's3://{script_bucket_name}/bootstrap_actions.sh'.format(
                            script_bucket_name=self.script_bucket_name),
                    }
                },
            ],
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole'
        )
        logger.info(response)
        return response

    def add_step(self, job_flow_id, master_dns):
        # First create your hive command line arguments
        hive_args = "hive -v -f s3://{script_bucket_name}/hive-schema.hql"

        # Split the hive args to a list
        hive_args_list = hive_args.split()
        response = self.boto_client("emr").add_job_flow_steps(
            JobFlowId=job_flow_id,
            Steps=[
                {
                    'Name': 'Setup Debugging',
                    'ActionOnFailure': 'TERMINATE_CLUSTER',
                    'HadoopJarStep': {
                    'Jar': 's3://ap-southeast-2.elasticmapreduce/libs/script-runner/script-runner.jar',
                    'Args': ['s3://ap-southeast-2.elasticmapreduce/libs/state-pusher/0.1/fetch']
                    }
                },
                {
                    'Name': 'setup - copy zeppelin config file',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['sudo','aws', 's3', 'cp',
                                    's3://{config_bucket_name}/zeppelin-site.xml'.format(
                                        config_bucket_name=self.config_bucket_name),
                                    '/etc/zeppelin/conf/']
                    }
                },
                {
                    'Name': 'setup - copy gbq access files',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['aws', 's3', 'cp',
                                 's3://{config_bucket_name}/google-api-credentials.json'.format(
                                     config_bucket_name=self.config_bucket_name),
                                 '/home/hadoop/']
                    }
                },
                {
                    'Name': 'setup - copy pyspark setup file',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['aws', 's3', 'cp',
                                 's3://{script_bucket_name}/pyspark_quick_setup.sh'.format(
                                     script_bucket_name=self.script_bucket_name),
                                 '/home/hadoop/']
                    }
                },
                {
                    'Name': 'setup pyspark with anaconda',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['sudo', 'bash', '/home/hadoop/pyspark_quick_setup.sh', master_dns]
                    }
                },
                {
                    'Name': 'setup - copy spark jar setup files',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['aws', 's3', 'cp',
                                 's3://{script_bucket_name}/reqd_files_setup.sh'.format(
                                     script_bucket_name=self.script_bucket_name),
                                 '/home/hadoop/']
                    }
                },
                {
                    'Name': 'copy spark jars to the spark folder',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['sudo', 'bash', '/home/hadoop/reqd_files_setup.sh', self.script_bucket_name]
                    }
                },
                {
                    'Name': 'hive-schema-setup',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': hive_args_list
                    }
                }

            ]
        )
        logger.info(response)
        return response

    def create_bucket_on_s3(self, bucket_name):
        s3 = self.boto_client("s3")
        try:
            logger.info("Bucket already exists.")
            s3.head_bucket(Bucket=bucket_name)
        except botocore.exceptions.ClientError as e:
            logger.info("Bucket does not exist: {error}. I will create it!".format(error=e))
            s3.create_bucket(Bucket=bucket_name)

    def upload_to_s3(self, file_name, bucket_name, key_name):
        s3 = self.boto_client("s3")
        logger.info(
            "Upload file '{file_name}' to bucket '{bucket_name}'".format(file_name=file_name, bucket_name=bucket_name))
        s3.upload_file(file_name, bucket_name, key_name)

    def uploadDirectory(self, local_directory, bucket_name):
        s3 = self.boto_client("s3")
        # enumerate local files recursively
        for root, dirs, files in os.walk(local_directory):
          for file in files:
            # construct the full local path
            file_name = os.path.join(root, file)
            logger.info(
                "Upload file '{file_name}' to bucket '{bucket_name}'".format(file_name=file_name, bucket_name=bucket_name))
            s3.upload_file(file_name, bucket_name, file)


def main():
    logger.info(
        "*******************************************+**********************************************************")
    logger.info("Load config and set up client.")
    with open("configs/config.yml", "r") as file:
        config = yaml.load(file)
    config_emr = config.get("emr")

    emr_loader = EMRLoader(
        aws_access_key=config_emr.get("aws_access_key"),
        aws_secret_access_key=config_emr.get("aws_secret_access_key"),
        region_name=config_emr.get("region_name"),
        cluster_name=config_emr.get("cluster_name"),
        instance_count=config_emr.get("instance_count"),
        master_instance_type=config_emr.get("master_instance_type"),
        slave_instance_type=config_emr.get("slave_instance_type"),
        key_name=config_emr.get("key_name"),
        subnet_id=config_emr.get("subnet_id"),
        log_uri=config_emr.get("log_uri"),
        software_version=config_emr.get("software_version"),
        script_bucket_name=config_emr.get("script_bucket_name"),
        config_bucket_name=config_emr.get("config_bucket_name"),
        db_username=config_emr.get("db_username"),
        db_password=config_emr.get("db_password")
    )

    logger.info(
        "*******************************************+**********************************************************")
    logger.info("Check if bucket exists otherwise create it and upload files to S3.")
    emr_loader.create_bucket_on_s3(bucket_name=config_emr.get("script_bucket_name"))
    emr_loader.upload_to_s3("scripts/bootstrap_actions.sh", bucket_name=config_emr.get("script_bucket_name"),
                            key_name="bootstrap_actions.sh")
    emr_loader.upload_to_s3("scripts/pyspark_quick_setup.sh", bucket_name=config_emr.get("script_bucket_name"),
                            key_name="pyspark_quick_setup.sh")
    emr_loader.upload_to_s3("scripts/hive-schema.hql", bucket_name=config_emr.get("script_bucket_name"),
                                key_name="hive-schema.hql")
    emr_loader.upload_to_s3("scripts/reqd_files_setup.sh", bucket_name=config_emr.get("script_bucket_name"),
                                key_name="reqd_files_setup.sh")
    emr_loader.uploadDirectory("files", bucket_name=config_emr.get("script_bucket_name"))
    emr_loader.create_bucket_on_s3(bucket_name=config_emr.get("config_bucket_name"))
    emr_loader.upload_to_s3("configs/google-api-credentials.json", bucket_name=config_emr.get("config_bucket_name"),
                                key_name="google-api-credentials.json")
    emr_loader.upload_to_s3("configs/zeppelin-site.xml", bucket_name=config_emr.get("config_bucket_name"),
                                key_name="zeppelin-site.xml")


    logger.info(
        "*******************************************+**********************************************************")
    logger.info("Create cluster and run boostrap.")
    emr_response = emr_loader.load_cluster()
    emr_client = emr_loader.boto_client("emr")

    while True:
        job_response = emr_client.describe_cluster(
            ClusterId=emr_response.get("JobFlowId")
        )
        time.sleep(10)
        if job_response.get("Cluster").get("MasterPublicDnsName") is not None:
            master_dns = job_response.get("Cluster").get("MasterPublicDnsName")

        step = True

        job_state = job_response.get("Cluster").get("Status").get("State")
        job_state_reason = job_response.get("Cluster").get("Status").get("StateChangeReason").get("Message")

        if job_state in ["WAITING","TERMINATED", "TERMINATED_WITH_ERRORS"]:
            step = False
            logger.info(
                "Script stops with state: {job_state} "
                "and reason: {job_state_reason}".format(job_state=job_state, job_state_reason=job_state_reason))
            break
        else:
            logger.info(job_response)

    if step:
        logger.info(
            "*******************************************+**********************************************************")
        logger.info("Run steps.")
        add_step_response = emr_loader.add_step(emr_response.get("JobFlowId"), master_dns)

        while True:
            list_steps_response = emr_client.list_steps(ClusterId=emr_response.get("JobFlowId"),
                                                        StepStates=["COMPLETED"])
            time.sleep(10)
            if len(list_steps_response.get("Steps")) == len(
                    add_step_response.get("StepIds")):  # make sure that all steps are completed
                break
            else:
                logger.info(emr_client.list_steps(ClusterId=emr_response.get("JobFlowId")))
    else:
        logger.info("Cannot run steps.")


if __name__ == "__main__":
    main()
