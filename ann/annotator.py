import os
import subprocess
import json

from botocore.config import Config
from botocore.exceptions import ClientError
import boto3


# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('ann_config.ini')

AwsRegionName = config['aws']['AwsRegionName']
TableName = config['aws']['TableName']
RequestsQueueURL = config['aws']['RequestsSQSURL']


def errortmp(self_defined_message, error_message):
    """Template for error message"""
    return self_defined_message + ' ' + str(error_message)


def main():
    """
    1. Poll the message queue, get a message and the job info
    2. Get the input file S3 object and copy it to a local file
    3. Launch annotation job as subprocess
    4. Update job_status to DynamoDB
    5. Delete the message
    """
    s3, db, sqs = None, None, None
    try:
        s3 = boto3.client('s3',
                          region_name=AwsRegionName,
                          config=Config(signature_version='s3v4')
                          )
        db = boto3.resource('dynamodb', region_name=AwsRegionName)
        sqs = boto3.client('sqs', region_name=AwsRegionName)
    except ClientError as e:
        print(errortmp("Get aws client failed.", e))

    while True:
        """Get uploaded file, annotate it and update job status to database"""
        # poll the message queue
        # reference:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.receive_message
        sqs_response = None
        try:
            sqs_response = sqs.receive_message(
                QueueUrl=RequestsQueueURL,
                AttributeNames=['All'],
                MaxNumberOfMessages=1,
                MessageAttributeNames=['All'],
                WaitTimeSeconds=3
            )
        except ClientError as e:
            print(errortmp("Poll the message queue failed.", e))

        if 'Messages' not in sqs_response:
            continue

        message = sqs_response['Messages'][0]  # only contains one message
        message_body = json.loads(json.loads(message["Body"])["Message"])
        # get the handler of this message in order to delete it
        receipt_handle = message["ReceiptHandle"]

        job_id = message_body['job_id']
        s3_inputs_bucket = message_body['s3_inputs_bucket']
        s3_key_input_file = message_body['s3_key_input_file']
        user_id = message_body['user_id']
        file_name = s3_key_input_file.split('/')[2]

        # if job_path folder does not exist, create one
        job_path = f"jobs/{user_id}/{job_id}"
        if not os.path.exists(job_path):
            os.makedirs(job_path)

        # download input vcf file from S3 to the annotator instance
        # reference:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-example-download-file.html
        try:
            s3.download_file(s3_inputs_bucket, s3_key_input_file, f'{job_path}/{file_name}')
        except ClientError as e:
            print(errortmp("Download file from S3 failed.", e))

        # perform annotation to the downloaded vcf file
        # command format: python /home/ec2-user/mpcs-cc/anntools/hw5_run.py <filename> <job_id>
        try:
            subprocess.Popen(['python',
                              '/home/ec2-user/mpcs-cc/gas/ann/run.py',
                              f'{job_path}/{file_name}',
                              job_id])
        except OSError as e:
            print(errortmp("Annotate the input file failed.", e))

        # update job_status to 'RUNNING' if the original status is 'PENDING'
        # reference:
        # https://highlandsolutions.com/blog/hands-on-examples-for-working-with-dynamodb-boto3-and-python
        try:
            table = db.Table(TableName)
            table.update_item(
                Key={'job_id': job_id},
                UpdateExpression='set job_status = :new_status',
                ConditionExpression='job_status = :cur_status',
                ExpressionAttributeValues={':new_status': 'RUNNING', ':cur_status': 'PENDING'},
                ReturnValues='UPDATED_NEW'
            )
        except ClientError as e:
            print(errortmp("Update job status failed.", e))

        # delete the message
        # reference:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.delete_message
        try:
            sqs.delete_message(QueueUrl=RequestsQueueURL,
                               ReceiptHandle=receipt_handle)
        except ClientError as e:
            print(errortmp("Delete message from queue failed.", e))


if __name__ == '__main__':
    main()
