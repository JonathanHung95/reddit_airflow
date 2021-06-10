from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from src.get_comments import get_comments
from src.get_posts import get_posts 
from src.write_csv import write_csv

import boto3

# simple file upload task

def upload_file(source, bucket, key):
    s3 = boto3.resource("s3")
    s3.meta.client.upload_file(source, key)

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

# define our S3 buckets

LANDING_BUCKET = "jonathan-landing-bucket"
TARGET_BUCKET = "jonathan-target-bucket"

# other s3 arguments needed

KEY = ""
FILE_PATH = ""

# default arguments

default_args = {
    'owner': 'Jonathan',
    'depends_on_past': False,
    'email': ['jonathan.aw.hung@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': YESTERDAY,
}

# set up the actual DAGs
# we need 2 here; 1st to run the python scrapping and 2nd to upload the resulting csv to our LANDING_BUCKET

with DAG(
    "reddit_scrapping",
    schedule_interval = @hourly,
    catchup = False,
    default_args = default_args
) as dag:

    scrap_data = PythonOperator(
        task_id = "scrap_data",
        provide_context = False,
        python_callable = main.py
    )

    upload_to_s3 = PythonOperator(
        task_id = "upload_to_s3",
        python_calleable = upload_to_s3,
        op_kwards = {"bucket": LANDING_BUCKET, "key": KEY, "source": FILE_PATH}
    )

scrap_data >> upload_to_s3