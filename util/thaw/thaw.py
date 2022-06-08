# thaw.py
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
from botocore.exceptions import ClientError

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.getcwd()))
import helpers

# Get configuration
from configparser import SafeConfigParser

config = SafeConfigParser(os.environ)
config.read('thaw/thaw_config.ini')


AwsRegionName = config['aws']['AwsRegionName']
RestoreCompleteSQSURL = config['aws']['RestoreCompleteSQSURL']
DynamoDBTableName = config['aws']['DynamoDBTableName']
GlacierVaultName = config['aws']['GlacierVaultName']


def main():
    s3, glacier, table, sqs = None, None, None, None
    try:
        s3 = boto3.client('s3', region_name=AwsRegionName)
        glacier = boto3.client('glacier', region_name=AwsRegionName)

        dynamodb = boto3.resource('dynamodb', region_name=AwsRegionName)
        table = dynamodb.Table(DynamoDBTableName)

        sqs = boto3.client('sqs', region_name=AwsRegionName)
    except ClientError as e:
        print("Get AWS clients failed.", str(e))

    while True:
        # poll message from restore complete queue
        sqs_response = None
        # reference:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.receive_message
        try:
            sqs_response = sqs.receive_message(
                QueueUrl=RestoreCompleteSQSURL,
                AttributeNames=['All'],
                MaxNumberOfMessages=1,
                MessageAttributeNames=['All'],
                WaitTimeSeconds=3
            )
        except ClientError as e:
            print("Poll the restore start queue failed.", str(e))

        if 'Messages' not in sqs_response:
            continue

        message = sqs_response['Messages'][0]
        receipt_handle = message['ReceiptHandle']
        message_body = json.loads(json.loads(message['Body'])['Message'])
        restore_job_id = message_body['JobId']
        archive_id = message_body['ArchiveId']
        job_id = json.loads(message_body['JobDescription'])['job_id']
        s3_results_bucket = json.loads(message_body['JobDescription'])['s3_results_bucket']
        s3_key_result_file = json.loads(message_body['JobDescription'])['s3_key_result_file']

        # get job output from Glacier
        # reference:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.get_job_output
        job_output = None
        try:
            glacier_response = glacier.get_job_output(vaultName=GlacierVaultName,
                                                      jobId=restore_job_id)
            job_output = glacier_response['body']
        except ClientError as e:
            print("Get job output from Glacier failed.", str(e))

        # restore result file from Glacier to S3
        # reference:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.upload_fileobj
        try:
            s3.upload_fileobj(job_output, s3_results_bucket, s3_key_result_file)
        except ClientError as e:
            print("Restore result file from Glacier to S3 failed.", str(e))

        # update "available_in_glacier" attribute in DynamoDB
        # reference:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.update_item
        try:
            table.update_item(
                Key={'job_id': job_id},
                UpdateExpression='set available_in_glacier = :new_available',
                ExpressionAttributeValues={':new_available': False},
                ReturnValues='UPDATED_NEW'
            )
        except ClientError as e:
            print("Update DynamoDB attribute failed.", str(e))

        # delete archive in Glacier
        # reference:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.delete_archive
        # "This operation is idempotent. Attempting to delete an already-deleted archive does not result in an error."
        try:
            glacier.delete_archive(vaultName=GlacierVaultName,
                                   archiveId=archive_id)
        except ClientError as e:
            print("Delete archive in Glacier failed.", e)

        # delete message from restore complete queue
        helpers.delete_message_from_sqs(sqs, RestoreCompleteSQSURL, receipt_handle)


if __name__ == '__main__':
    main()
