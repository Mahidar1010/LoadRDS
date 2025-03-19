# Simple ETL Pipeline Using AWS S3, Lambda, and RDS (MySQL)

## 📌 **Project Overview**
This project demonstrates a simple **ETL (Extract, Transform, Load)** pipeline using AWS services. The pipeline automates the process of extracting a CSV file from Amazon S3, transforming the data using AWS Lambda, and loading it into an Amazon RDS (MySQL) database. It also includes error handling with Amazon SNS for notifications and secure management of credentials using AWS Secrets Manager and Systems Manager Parameter Store.

---

## 🛠️ **Architecture Overview**

- **Data Source:** CSV file uploaded from a local machine to an S3 bucket using AWS CLI.
- **Processing:** AWS Lambda function reads the CSV from S3, performs data validation, and loads the data into an Amazon RDS (MySQL) table.
- **Storage:** Amazon S3 stores the raw CSV file, while Amazon RDS stores the processed data.
- **Error Notification:** Amazon SNS sends alerts in case of any processing failures.
- **Secrets Management:** AWS Secrets Manager securely stores database credentials.
- **Parameter Management:** AWS Systems Manager Parameter Store stores the database hostname.

---

## 🚀 **Technologies Used**
- **AWS S3** for storing raw data
- **AWS Lambda** for serverless processing
- **Amazon RDS (MySQL)** for relational data storage
- **Amazon SNS** for error notifications
- **AWS Secrets Manager** for credential management
- **AWS Systems Manager (SSM)** for managing database hostname
- **SQLAlchemy** for database interaction
- **Boto3** for AWS service interaction using Python
- **Pandas** for data processing
- **DBeaver** for database management

---

## 📥 **Prerequisites**

Ensure you have the following tools and AWS resources configured:
- AWS CLI installed and configured
- Python 3.8 or above
- Boto3 and SQLAlchemy installed (`pip install boto3 pandas sqlalchemy pymysql`)
- Access to AWS Management Console
- Amazon RDS instance (MySQL engine)
- S3 bucket with proper permissions
- AWS Secrets Manager configured for storing RDS credentials
- AWS Systems Manager Parameter Store with database hostname

---

## 🛠️ **Step 1: Upload CSV File to S3**
1. Ensure your AWS CLI is configured.
2. Run the following command to upload your CSV to S3:

```bash
aws s3 cp Salescsvfile.csv s3://bucketname/sales_raw/
```

---

## 🧑‍💻 **Step 2: Create AWS Resources**

### ✅ **1. Create an RDS MySQL Instance:**
- Go to **Amazon RDS** in the AWS Management Console.
- Create a new MySQL database.
- Note down the database endpoint, username, and password.

### ✅ **2. Create a Secrets Manager Entry:**
- Navigate to **AWS Secrets Manager**.
- Create a new secret with database credentials.
- Save the secret name (`dev/rds/database1`).

### ✅ **3. Store Database Hostname in SSM:**
- Go to **AWS Systems Manager → Parameter Store**.
- Create a new parameter named `dev/rds/hostname`.
- Add your RDS endpoint as the value.

### ✅ **4. Create an SNS Topic:**
- Go to **Amazon SNS**.
- Create a new topic named `dev_sales`.
- Add your email address as a subscriber.

---

## 📝 **Step 3: AWS Lambda Code**

Here’s the core logic of the Lambda function:

1. **Extract:** Fetches data from S3 using Boto3.
2. **Transform:** Validates and processes data using Pandas.
3. **Load:** Inserts the data into the MySQL table using SQLAlchemy.
4. **Error Handling:** Sends notifications using SNS on failures.

### **Environment Variables:**
- `SECRET_NAME` = `dev/rds/database1`
- `SNS_TOPIC_NAME` = `dev_sales`
- `S3_BUCKET_NAME` = `MyBucketName`
- `S3_KEY` = `sales_raw/Salescsvfile.csv`
- `SSM_PARAMETER_NAME` = `dev/rds/hostname`

### **Lambda Handler Example:**
```python
import pandas as pd
import boto3
import json
from sqlalchemy import create_engine
from botocore.exceptions import BotoCoreError, NoCredentialsError
import pymysql

session = boto3.Session()
secrets_client = session.client("secretsmanager")
sns_client = session.client("sns")
s3_client = session.client("s3")
sts_client = session.client("sts")
ssm_client = session.client("ssm")

# Functions to fetch secrets and parameters, read from S3, and write to RDS
# Error handling using SNS notifications
```

---

## 📊 **Step 4: Monitoring and Alerts**
- Monitor Lambda function logs using **AWS CloudWatch**.
- Check for SNS notifications in case of errors.
- Ensure data accuracy using SQL queries in DBeaver.

---

## 📦 **Step 5: Clean Up**
- Delete unused S3 objects to save storage costs.
- Remove RDS instances if not needed.
- Unsubscribe from SNS topics.

---

## 📣 **Future Enhancements**
- Add data quality checks in the Lambda function.
- Implement incremental data loading.
- Visualize data using AWS QuickSight.

---

## 💬 **Contributing**
Feel free to fork this repository, raise issues, or submit pull requests to improve the project.

---

## 📧 **Contact**
For any questions, feel free to reach out via GitHub or connect with me on [LinkedIn](https://linkedin.com/in/mahidar-reddy-putta).

