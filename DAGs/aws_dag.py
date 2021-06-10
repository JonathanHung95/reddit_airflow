from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.operators import PythonOperator
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

def _local_to_s3(filename, key, bucket_name = BUCKET_NAME):
    s3 = S3Hook()
    s3.load_file(filename = filename, bucket_name = bucket_name, replace = True, key = key)

# dag to execute the function to move our generated local data to the s3 bucket

def data_to_s3 = PythonOperator (
    dag = dag,
    task_id = "data_to_s3",
    python_callable = local_to_s3,
    op_kwargs = {"filename": local_data, "key" = s3_data,},
)

# spark steps for the spark job?

SPARK_STEPS = [
    {
        "Name": "Move data from local to "
    }
]
