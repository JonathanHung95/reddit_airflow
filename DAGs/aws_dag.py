from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.operators import PythonOperator, DummyOperator
from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)

import airflow.hooks.s3_hook import S3Hook
from airflow.operators import PythonOperator

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['jonathan.aw.hung@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

# some variables for aws

CLUSTER_ID = ""
BUCKET_NAME = "jonathan-landing-bucket" 
local_data = "comments.csv"
s3_data = "data/comments.csv"
s3_script = "jonathan-scripts/main.py"
s3_clean = "jonathan-target-bucket"

# helper functions

def local_to_s3(filename, key, bucket_name = BUCKET_NAME):
    s3 = S3Hook()
    s3.load_file(filename = filename, bucket_name = bucket_name, replace = True, key = key)

# spark steps for the spark job?

SPARK_STEPS = [
    {
        "Name": "Move raw data from S3 to HDFS",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=s3://jonathan-landing-bucket",
                "--dest=/comments",
            ],
        },
    },
    {
        "Name": "Classify movie reviews",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://jonathan-scripts/spark_job.py",
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
                "--dest=s3://jonathan-target-bucket/clean_data",
            ],
        },
    },
]


dag = DAG(
    "emr_job_flow",
    schedule_interval = @hourly,
    catchup = False,
    dagrun_timeout = timedelta(hours = 1),
    default_args = DEFAULT_ARGS
)

# dummy start

start_pipeline = DummyOperator(task_id = "start_pipeline",
                                dag = dag)

# move data from local -> s3 -> EMR picks up from s3 -> run spark job -> deposit transformed data into s3

data_to_s3 = PythonOperator(
    dag = dag,
    task_id = "data_to_s3",
    python_callable = local_to_s3,
    op_kwargs = {"filename": local_data, "key" = s3_data,},
)

"""
parse_request = PythonOperator(
    task_id = "parse_request",
    provide_context = True,
    python_callable = retrieve_s3_file,
    dag = dag
)
"""

step_adder = EmrAddStepsOperator(
    task_id = "add_steps",
    job_flow_id = CLUSTER_ID,
    aws_conn_id = "aws_default",
    steps = SPARK_STEPS,
    dag = dag
)

step_checker = EmrStepSensor(
    task_id = "watch_step",
    job_flow_id = CLUSTER_ID,
    step_id = "{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
    aws_conn_id = "aws_default",
    dag = dag
)

end_pipeline = DummyOperator(task_id = "end_pipeline",
                                dag = dag)

start_pipeline >> data_to_s3 >> step_adder >> step_checker >> end_pipeline