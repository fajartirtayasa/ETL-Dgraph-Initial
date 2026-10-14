#Import the required libraries
import os
import yaml
import boto3
import json
from botocore.exceptions import ClientError


#CONFIG FUNCTIONS ----------------------------------------------------------------------------------------

#function to read the YAML file
def load_yaml(env: str):
    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    
    env_config = config.get(env)
    if not env_config:
        raise ValueError(f"Environment '{env}' not found in the config file.")

    # Set environment variables
    for key, value in env_config.items():
        if isinstance(value, list):
            # Convert list to comma-separated string for environment variable
            value = ','.join(value)
        elif isinstance(value, dict):
            # Convert dict to JSON string for environment variable
            value = str(value)
        os.environ[key] = str(value)

#function to get credentials from AWS Secret Manager
def get_secret():
    secret_name = "etl_cred_prd"
    region_name = "ap-southeast-3"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    # Decode secret as JSON
    secret = json.loads(get_secret_value_response['SecretString'])

    # Extract credentials
    aws_access_key = secret['AWS_ACCESS_KEY']
    aws_secret_key = secret['AWS_SECRET_KEY']
    host = 'localhost'
    port = secret['PORT']
    user = secret['USER']
    password = secret['PASSWORD']
    db_raw = secret['DB_RAW']
    db_clean = secret['DB_CLEAN']
    report_recipients = secret['REPORT_RECIPIENTS']
    report_custom_date = secret['REPORT_CUSTOM_DATE']

    db_conn_raw = {
        'host': host,
        'port': port,
        'database': db_raw,
        'user': user,
        'password': password
    }
    db_conn_clean = {
        'host': host,
        'port': port,
        'database': db_clean,
        'user': user,
        'password': password
    }

    return aws_access_key, aws_secret_key, db_conn_raw, db_conn_clean, report_recipients, report_custom_date