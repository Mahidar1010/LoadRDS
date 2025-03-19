import pandas as pd
import boto3
import json
from sqlalchemy import create_engine
from botocore.exceptions import BotoCoreError, NoCredentialsError
import pymysql

# AWS configurations
SECRET_NAME = "dev/rds/database1"  # Your secret name in AWS Secrets Manager
AWS_REGION = "us-east-1"  # Your AWS region
SNS_TOPIC_NAME = "dev_sales"  # Your SNS topic name
CSV_FILE_PATH = r"C:\Users\keert\OneDrive\Desktop\RADE\Salescsvfile.csv"  # Your CSV file path

# Initialize AWS session
session = boto3.Session()
secrets_client = session.client(service_name="secretsmanager", region_name=AWS_REGION)
sns_client = session.client("sns", region_name=AWS_REGION)
sts_client = session.client("sts")

def get_aws_account_id():
    """Fetches the AWS account ID dynamically."""
    try:
        account_id = sts_client.get_caller_identity()["Account"]
        return account_id
    except BotoCoreError as e:
        raise Exception(f"Failed to fetch AWS account ID: {str(e)}")

def get_sns_topic_arn():
    """Constructs SNS Topic ARN dynamically."""
    account_id = get_aws_account_id()
    return f"arn:aws:sns:{AWS_REGION}:{account_id}:{SNS_TOPIC_NAME}"

def send_sns_notification(subject, message):
    """Publishes an SNS notification."""
    try:
        sns_topic_arn = get_sns_topic_arn()
        sns_client.publish(TopicArn=sns_topic_arn, Subject=subject, Message=message)
        print(f"SNS Notification sent: {subject}")
    except BotoCoreError as e:
        print(f"Failed to send SNS notification: {str(e)}")

def get_database_credentials():
    """Fetches database credentials from AWS Secrets Manager."""
    try:
        response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
        secrets = json.loads(response["SecretString"])
        return secrets
    except (BotoCoreError, NoCredentialsError) as e:
        send_sns_notification("Sales Job Failed - Database Credentials Error", f"Failed to retrieve database credentials: {str(e)}")
        raise

def load_csv_to_dataframe(file_path):
    """Loads the CSV file into a Pandas DataFrame."""
    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        send_sns_notification("Sales Job Failed - CSV Read Error", f"Error reading CSV file: {str(e)}")
        raise

def connect_to_database(credentials):
    """Creates a database connection using credentials from Secrets Manager."""
    try:
        engine = create_engine(f"mysql+pymysql://{credentials['username']}:{credentials['password']}@{credentials['host']}:{credentials['port']}/{credentials['dbname']}")
        return engine
    except Exception as e:
        send_sns_notification("Sales Job Failed - Database Connection Error", f"Database connection error: {str(e)}")
        raise

def insert_data_to_rds(df, engine):
    """Inserts DataFrame data into RDS MySQL table."""
    try:
        df.to_sql("Sale", con=engine, if_exists="replace", index=False)
        print("Data inserted successfully into AWS RDS MySQL database!")
    except Exception as e:
        send_sns_notification("Sales Job Failed - Data Insert Error", f"Error inserting data into RDS: {str(e)}")
        raise

# Main execution
if __name__ == "__main__":
    try:
        credentials = get_database_credentials()  # Explicitly handle failures in fetching credentials
        df = load_csv_to_dataframe(CSV_FILE_PATH)
        engine = connect_to_database(credentials)
        insert_data_to_rds(df, engine)
    except Exception as e:
        print(f"Job failed: {str(e)}")
