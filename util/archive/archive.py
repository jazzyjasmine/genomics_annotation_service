# archive.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
import json

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.getcwd()))
import helpers

# Get configuration
from configparser import SafeConfigParser

config = SafeConfigParser(os.environ)
config.read('archive/archive_config.ini')

AwsRegionName = config['aws']['AwsRegionName']
GlacierVaultName = config['aws']['GlacierVaultName']
DynamoDBTableName = config['aws']['DynamoDBTableName']
ArchiveSQSURL = config['aws']['ArchiveSQSURL']


def main():
    s3, glacier, sqs, table = None, None, None, None
    try:
        s3 = boto3.client('s3',
                          region_name=AwsRegionName,
                          config=Config(signature_version='s3v4')
                          )
        glacier = boto3.client('glacier', region_name=AwsRegionName)

        dynamodb = boto3.resource('dynamodb', region_name=AwsRegionName)
        table = dynamodb.Table(DynamoDBTableName)

        sqs = boto3.client('sqs', region_name=AwsRegionName)
    except ClientError as e:
        print("Get AWS clients failed.", str(e))

    while True:
        # poll job info from SQS archive
        # The archive SQS is a delayed queue, the "delivery delay" is set to be 5 minutes
        # reference:
        # https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-delay-queues.html
        sqs_response = None
        try:
            sqs_response = sqs.receive_message(
                QueueUrl=ArchiveSQSURL,
                AttributeNames=['All'],
                MaxNumberOfMessages=1,
                MessageAttributeNames=['All'],
                WaitTimeSeconds=2
            )
        except ClientError as e:
            print("Poll the message queue failed.", str(e))

        if 'Messages' not in sqs_response:
            continue

        message = sqs_response['Messages'][0]  # only contains one message
        message_body = json.loads(json.loads(message["Body"])["Message"])
        # get the handler of this message in order to delete it
        receipt_handle = message["ReceiptHandle"]

        job_id = message_body['job_id']
        s3_results_bucket = message_body['s3_results_bucket']
        s3_key_result_file = message_body['s3_key_result_file']
        user_id = message_body['user_id']

        # check user type (free or premium)
        user_type = helpers.get_user_profile(user_id)[0][4]
        # delete message and continue if premium user
        if user_type == 'premium_user':
            helpers.delete_message_from_sqs(sqs, ArchiveSQSURL, receipt_handle)
            continue

        # for free users:
        # get S3 result file
        s3_response = None
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object
        try:
            s3_response = s3.get_object(Bucket=s3_results_bucket, Key=s3_key_result_file)
        except ClientError as e:
            print("Get S3 result file failed.", str(e))

        # upload result file to Glacier
        glacier_upload_response = None
        # reference:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.upload_archive
        try:
            glacier_upload_response = glacier.upload_archive(vaultName=GlacierVaultName,
                                                             archiveDescription=job_id,
                                                             body=s3_response['Body'].read()
                                                             )
        except ClientError as e:
            print("Upload result file to Glacier failed.", str(e))

        # get results_file_archive_id and update it to DynamoDB
        results_file_archive_id = glacier_upload_response['ResponseMetadata']['HTTPHeaders']['x-amz-archive-id']
        # reference:
        # https://highlandsolutions.com/blog/hands-on-examples-for-working-with-dynamodb-boto3-and-python
        try:
            table.update_item(
                Key={'job_id': job_id},
                UpdateExpression='set results_file_archive_id = :new_archive_id, available_in_glacier = :new_available',
                ExpressionAttributeValues={':new_archive_id': results_file_archive_id,
                                           ':new_available': True},
                ReturnValues='UPDATED_NEW'
            )
        except ClientError as e:
            print("Update results_file_archive_id to DynamoDB failed.", str(e))

        # delete result file from S3
        # reference:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.delete_object
        try:
            s3.delete_object(Bucket=s3_results_bucket, Key=s3_key_result_file)
        except ClientError as e:
            print("Delete result file from S3 failed.", str(e))

        # delete message from archive SQS
        helpers.delete_message_from_sqs(sqs, ArchiveSQSURL, receipt_handle)


if __name__ == '__main__':
    main()
