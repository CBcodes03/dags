from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import os

# DAG-level constants
QUEUE_NAME = 'test'
WARNING_THRESHOLD = 10
CRITICAL_THRESHOLD = 20

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def check_sqs_queue():
    # Get credentials from environment
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    region = os.getenv('AWS_DEFAULT_REGION', 'ap-south-1')
    print("AWS_ACCESS_KEY_ID:", aws_access_key)
    print("AWS_SECRET_ACCESS_KEY:", aws_secret_key)
    if not aws_access_key or not aws_secret_key:
        raise ValueError("AWS credentials not found in environment variables.")

    # Initialize Boto3 with environment credentials
    session = boto3.Session(
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region,
    )
    sqs = session.resource('sqs')
    
    try:
        queue = sqs.get_queue_by_name(QueueName=QUEUE_NAME)
        msgs = int(queue.attributes.get('ApproximateNumberOfMessages', 0))

        if msgs < WARNING_THRESHOLD:
            print(f"OK - working fine: {msgs}")
        elif WARNING_THRESHOLD <= msgs < CRITICAL_THRESHOLD:
            print(f"WARNING - threshold exceeded: {msgs}")
        else:
            print(f"CRITICAL - critical threshold exceeded: {msgs}")

    except sqs.meta.client.exceptions.QueueDoesNotExist:
        raise ValueError(f"Queue '{QUEUE_NAME}' does not exist.")
    except Exception as e:
        raise RuntimeError(f"Error checking SQS queue: {str(e)}")

with DAG(
    dag_id='check_sqs_queue',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='* * * * *',
    catchup=False,
    tags=['sqs', 'monitoring'],
) as dag:

    check_sqs = PythonOperator(
        task_id='check_sqs_status',
        python_callable=check_sqs_queue
    )

