import time

import boto3
from botocore.exceptions import ClientError

import sys
import uuid
import json
import os

from configparser import SafeConfigParser

config = SafeConfigParser(os.environ)
config.read('ann_load_config.ini')

AwsRegionName = config['aws']['AwsRegionName']
TableName = config['aws']['TableName']
HardcodedUserId = config['aws']['HardcodedUserId']
HardcodedInputFileName = config['aws']['HardcodedInputFileName']
S3InputsBucket = config['aws']['S3InputsBucket']
S3KeyPrefix = config['aws']['S3KeyPrefix']
RequestsSNSArn = config['aws']['RequestsSNSArn']


def simulate_job_submissions(message_count):
    """Simulate multiple job submissions to do load test on annotator farm"""
    table, sns = None, None
    try:
        sns = boto3.client('sns', region_name=AwsRegionName)
        dynamo = boto3.resource('dynamodb', region_name=AwsRegionName)
        table = dynamo.Table(TableName)
    except ClientError as e:
        print("Connect to AWS failed.", str(e))

    for i in range(message_count):
        job_id = str(uuid.uuid4())
        print("job_id", job_id)
        job_data = {
            'job_id': job_id,
            'user_id': HardcodedUserId,
            'input_file_name': HardcodedInputFileName,
            's3_inputs_bucket': S3InputsBucket,
            's3_key_input_file': f'{S3KeyPrefix}/{HardcodedUserId}/{job_id}~{HardcodedInputFileName}',
            'submit_time': int(time.time()),
            'job_status': 'PENDING'
        }

        try:
            table.put_item(Item=job_data)
        except ClientError as e:
            print("Put job info to DynamoDB failed.", str(e))

        try:
            sns.publish(TopicArn=RequestsSNSArn,
                        Message=json.dumps({'default': json.dumps(job_data)}),
                        MessageStructure='json')
        except ClientError as e:
            print("Publish notification message failed.", e)

        time.sleep(5)


if __name__ == '__main__':
    simulate_job_submissions(int(sys.argv[1]))
