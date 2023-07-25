import boto3
import json
import logging
from botocore.exceptions import BotoCoreError, ClientError
from sqlithree.config import settings

def create_s3_bucket(bucket_name, region=None):
    """
    Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String, optional. Region to create the bucket in.
    :return: True if bucket created, else False
    """

    client = boto3.client('s3')

    # Create the S3 bucket
    client.create_bucket(Bucket=bucket_name)

    # Set the CORS policy
    cors_configuration = {
        'CORSRules': [{
            'AllowedHeaders': ['*'],
            'AllowedMethods': ['GET'],
            'AllowedOrigins': ['*'],
        }]
    }
    client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors_configuration)

    # Set the bucket policy
    bucket_policy = {
        'Statement': [{
            'Effect': 'Allow',
            'Principal': '*',
            'Action': 's3:*',
            'Resource': f'arn:aws:s3:::{bucket_name}/*'
        }]
    }

    # Convert the policy to a JSON-formatted string
    bucket_policy = json.dumps(bucket_policy)

    client.put_bucket_policy(Bucket=bucket_name, Policy=bucket_policy)


create_s3_bucket(settings.sqlithree_bucket_name)
