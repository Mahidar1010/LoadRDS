import pandas as pd
import boto3
import json
from sqlalchemy import create_engine
from botocore.exceptions import BotoCoreError, NoCredentialsError
import pymysql
from io import StringIO
import os

# AWS configurations
SECRET_NAME = os.getenv("SECRET_NAME")  # Your secret name in AWS Secrets Manager
AWS_REGION = "us-east-1"  # Your AWS region
SNS_TOPIC_NAME = os.getenv("SNS_TOPIC_NAME")  # Your SNS topic name
SSM_PARAMETER_NAME = os.getenv("SSM_PARAMETER_NAME")

# Initialize AWS clients
session = boto3.Session()
secrets_client = session.client(service_name="secretsmanager", region_name=AWS_REGION)
sns_client = session.client("sns", region_name=AWS_REGION)
s3_client = session.client("s3")
sts_client = session.client("sts")
ssm_client = session.client("ssm", region_name=AWS_REGION)


def get_aws_account_id():
    """Fetch AWS account ID dynamically."""
    try:
        account_id = sts_client.get_caller_identity()["Account"]
        return account_id
    except BotoCoreError as e:
        raise Exception(f"Failed to fetch AWS account ID: {str(e)}")

def get_sns_topic_arn():
    """Generate SNS topic ARN dynamically using the account ID."""
    account_id = get_aws_account_id()
    return f"arn:aws:sns:{AWS_REGION}:{account_id}:{SNS_TOPIC_NAME}"

def send_sns_notification(subject, message):
    """Send an SNS notification in case of failure."""
    try:
        sns_topic_arn = get_sns_topic_arn()
        sns_client.publish(TopicArn=sns_topic_arn, Subject=subject, Message=message)
        print(f"SNS Notification sent: {subject}")
    except BotoCoreError as e:
        print(f"Failed to send SNS notification: {str(e)}")

def get_database_credentials():
    """Retrieve database credentials from AWS Secrets Manager."""
    try:
        response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
        secrets = json.loads(response["SecretString"])
        ssm_response = ssm_client.get_parameter(Name=SSM_PARAMETER_NAME, WithDecryption=False)
        secrets['host'] = ssm_response['Parameter']['Value']
        return secrets
    except (BotoCoreError, NoCredentialsError) as e:
        send_sns_notification("Sales Job Failed - Database Credentials Error", f"Failed to retrieve database credentials: {str(e)}")
        raise

def load_csv_from_s3(bucket_name, key):
    """Read CSV from S3 using StringIO and convert to DataFrame."""
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        csv_data = response["Body"].read().decode("utf-8")
        df = pd.read_csv(StringIO(csv_data))
        print("Successfully read data from S3.")
        return df
    except Exception as e:
        send_sns_notification("Sales Job Failed - S3 File Read Error", f"Error reading file from S3: {str(e)}")
        raise

def connect_to_database(credentials):
    """Establish connection to RDS using SQLAlchemy."""
    try:
        engine = create_engine(f"mysql+pymysql://{credentials['username']}:{credentials['password']}@{credentials['host']}:{credentials['port']}/{credentials['dbname']}")
        print("Database connection established.")
        return engine
    except Exception as e:
        send_sns_notification("Sales Job Failed - Database Connection Error", f"Database connection error: {str(e)}")
        raise

def insert_data_to_rds(df, engine):
    """Insert DataFrame data into RDS table."""
    try:
        df.to_sql("Sale", con=engine, if_exists="replace", index=False)
        print("Data inserted successfully into AWS RDS MySQL database!")
    except Exception as e:
        send_sns_notification("Sales Job Failed - Data Insert Error", f"Error inserting data into RDS: {str(e)}")
        raise

# Lambda Handler
def lambda_handler(event, context):
    try:
        # Extract S3 bucket name and object key from event
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        file_key = event['Records'][0]['s3']['object']['key']

        print(f"Bucket: {bucket_name}, File: {file_key}")

        # Fetch credentials, read data, and insert into RDS
        credentials = get_database_credentials()
        df = load_csv_from_s3(bucket_name, file_key)
        engine = connect_to_database(credentials)
        insert_data_to_rds(df, engine)
        print("Lambda execution completed successfully.")
    except Exception as e:
        print(f"Job failed: {str(e)}")

