from airflow import DAG
from airflow.decorators import task, dag
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import datetime
import boto3
from botocore.exceptions import WaiterError

# AWS credentials
aws_access_key_id = Variable.get('aws_access_key_id')
aws_secret_access_key = Variable.get('aws_secret_access_key')

# AWS EMR client
client = boto3.client(
    'emr', 
    region_name='us-east-2',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

# Default arguments for the DAG
default_args = {
    'owner': 'Daniel',
    'start_date': datetime(2024, 1, 4)
}

@dag(default_args=default_args, 
     schedule_interval="@once",  
     description="Executes a Spark job on EMR",
     catchup=False, 
     tags=['Spark', 'EMR'])
def indicadores_enem():

    # Define dummy tasks for DAG
    inicio = DummyOperator(task_id='inicio')
    fim = DummyOperator(task_id="fim")

    @task
    def create_emr_cluster():
        """Task to create an EMR cluster."""
        try:
            cluster_id = client.run_job_flow(
                Name="indicadores-titanic-cluster",
                LogUri="s3://aws-logs-197398273774-us-east-2/elasticmapreduce/", 
                ReleaseLabel="emr-6.8.0",  # Specify EMR release version
                Instances={
                    'InstanceGroups': [
                        {
                            'Name': 'Master Node',
                            'Market': 'ON_DEMAND',
                            'InstanceRole': 'MASTER',
                            'InstanceType': 'm5.xlarge',
                            'InstanceCount': 1
                        },
                        {
                            'Name': 'Worker Nodes',
                            'Market': 'ON_DEMAND',
                            'InstanceRole': 'CORE',
                            'InstanceType': 'm5.xlarge',
                            'InstanceCount': 1
                        }
                    ],
                    'Ec2KeyName': 'spark-airflow-197398273774',  # Specify EC2 key pair
                    'KeepJobFlowAliveWhenNoSteps': True,
                    'TerminationProtected': False
                },
                Applications=[
                    {'Name': 'Hadoop'},
                    {'Name': 'Spark'}
                ],
                VisibleToAllUsers=True,
                ServiceRole='arn:aws:iam::197398273774:role/service-role/AmazonEMR-ServiceRole-20240410T144521',
                JobFlowRole='arn:aws:iam::197398273774:instance-profile/AmazonEMR-InstanceProfile-20240410T144504',
            )['JobFlowId']
            return cluster_id
        except Exception as e:
            raise ValueError(f"Failed to create EMR cluster: {str(e)}")

    @task
    def wait_emr_cluster(cluster_id):
        """Task to wait for EMR cluster to reach 'WAITING' state."""
        try:
            waiter = client.get_waiter('cluster_running')
            waiter.wait(
                ClusterId=cluster_id,
                WaiterConfig={
                    'Delay': 30,
                    'MaxAttempts': 60
                }
            )
        except WaiterError as we:
            raise ValueError(f"Failed to wait for EMR cluster: {str(we)}")

    @task
    def emr_process_enem():
        """Task to submit a Spark job to process ENEM data on EMR."""
        try:
            # Submit Spark job to EMR cluster
            newstep = client.add_job_flow_steps(
                JobFlowId="j-1A6PUI33BIXW3",  # Replace with your EMR cluster ID
                Steps=[
                    {
                        'Name': 'Processa indicadores Enem',
                        'ActionOnFailure': "CONTINUE",
                        'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': ['spark-submit',
                                    '--master', 'yarn',
                                    '--deploy-mode', 'cluster',
                                    's3://projeto-aws-eng-dados-dmc-spark/codigo/spark_code.py'
                                    ]
                        }
                    }
                ]
            )
            return newstep['StepIds'][0]
        except Exception as e:
            raise ValueError(f"Failed to submit Spark job to EMR: {str(e)}")

    # Define task dependencies
    create_cluster_task = create_emr_cluster()
    wait_cluster_task = wait_emr_cluster(create_cluster_task)
    process_enem_task = emr_process_enem()

    inicio >> create_cluster_task >> wait_cluster_task >> process_enem_task >> fim

# Instantiate the DAG
execucao = indicadores_enem()
