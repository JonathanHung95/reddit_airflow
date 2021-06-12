import os
import boto3
from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.lambda_function import AwsLambdaHook

from airflow.contrib.operators.emr_create_job_flow_operator \
        import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator \
        import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator \
        import EmrTerminateJobFlowOperator


# try to build the dag step-by-step this time

# important details

DEFAULT_ARGS = {
    'owner': 'Jonathan',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['jonathan.aw.hung@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

REGION_NAME = "us-east-1"

AWS_ACCESS_KEY_ID = "AKIASVOJD6ASJKM6GCVL"
AWS_SECRET_ACCESS_KEY = "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"

BUCKET_NAME = "jonathan-landing-bucket"
CLUSTER_ID = "j-2GT7P2DHKTF55"

# helper functions

def lambda1(ds, **kwargs):
    lambda_client = boto3.client("lambda",
                                    region_name = REGION_NAME,
                                    aws_access_key_id = AWS_ACCESS_KEY_ID,
                                    aws_secret_access_key = AWS_SECRET_ACCESS_KEY)

    response_1 = lambda_client.invoke(FunctionName = "reddit_scrapping",
                                        InvocationType = "RequestResponse")
    print("Response: ", response_1)
"""

def lambda1(ds, **kwargs):
    hook = AwsLambdaHook("reddit_scrapping",
                            region_name = REGION_NAME,
                            log_type = "None",
                            qualifier = "$LATEST",
                            incovation_type = "RequestResponse",
                            config = None,
                            aws_conn_id = "my_aws_conn")
    
    response_1 = hook.invoke_lambda(payload = "null")
    print("Response: ", response_1)
"""

def retrieve_s3_file(**kwargs):
    s3_location = "s3://jonathan-wcd-midterm/landing"
    kwargs['ti'].xcom_push( key = 's3location', value = s3_location)

# spark steps

SPARK_STEPS = [
    {
        "Name": "Move raw data from S3 to HDFS",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=s3://jonathan-wcd-midterm/landing",
                "--dest=/data",
            ],
        },
    },
    {
        "Name": "Transform Data",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://jonathan-wcd-midterm/scripts/spark_job.py",
            ],
        },
    },
    {
        "Name": "Move clean data from HDFS to S3",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=/output",
                "--dest=s3://jonathan-wcd-midterm/target",
            ],
        },
    },
]

# main dag section

dag = DAG(
    "a_new_dag_job",
    schedule_interval = "@hourly",
    catchup = False,
    dagrun_timeout = timedelta(hours = 1),
    default_args = DEFAULT_ARGS
)

start_pipeline = DummyOperator(
    task_id = "start_pipeline",
    dag = dag
)

run_lambda = PythonOperator(
    task_id = "run_lambda",
    python_callable = lambda1,
    provide_context = True,
    dag = dag
)


step_adder = EmrAddStepsOperator(
    task_id = "add_steps",
    #job_flow_id = "{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
    job_flow_id = CLUSTER_ID,
    aws_conn_id = "my_aws_conn",
    steps = SPARK_STEPS,
    dag = dag
)

last_step = len(SPARK_STEPS) - 1
step_checker = EmrStepSensor(
    task_id = "watch_step",
    #job_flow_id = "{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    job_flow_id = CLUSTER_ID,
    step_id = "{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
    + str(last_step)
    + "] }}",
    aws_conn_id = "my_aws_conn",
    dag=dag,
)

end_pipeline = DummyOperator(
    task_id = "end_pipeline",
    dag = dag
)

start_pipeline >> run_lambda >> step_adder >> step_checker >> end_pipeline
