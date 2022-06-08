# restore.py
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
from boto3.dynamodb.conditions import Key

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.getcwd()))
import helpers

# Get configuration
from configparser import SafeConfigParser

config = SafeConfigParser(os.environ)
config.read('restore/restore_config.ini')

AwsRegionName = config['aws']['AwsRegionName']
RestoreStartSQSURL = config['aws']['RestoreStartSQSURL']
DynamoDBTableName = config['aws']['DynamoDBTableName']
RestoreCompleteSNSArn = config['aws']['RestoreCompleteSNSArn']
GlacierVaultName = config['aws']['GlacierVaultName']


def main():
    glacier, sqs, table = None, None, None
    try:
        glacier = boto3.client('glacier', region_name=AwsRegionName)

        dynamodb = boto3.resource('dynamodb', region_name=AwsRegionName)
        table = dynamodb.Table(DynamoDBTableName)

        sqs = boto3.client('sqs', region_name=AwsRegionName)
    except ClientError as e:
        print("Get AWS clients failed.", str(e))

    while True:
        # poll message from restore start SQS (get user_id)
        sqs_response = None
        try:
            sqs_response = sqs.receive_message(
                QueueUrl=RestoreStartSQSURL,
                AttributeNames=['All'],
                MaxNumberOfMessages=1,
                MessageAttributeNames=['All'],
                WaitTimeSeconds=3
            )
        except ClientError as e:
            print("Poll the restore start queue failed.", str(e))

        if 'Messages' not in sqs_response:
            continue

        message = sqs_response['Messages'][0]  # only contains one message
        message_body = json.loads(json.loads(message["Body"])["Message"])
        receipt_handle = message["ReceiptHandle"]  # get the handler of this message in order to delete it later
        user_id = message_body['user_id']

        # get archive_ids from DynamoDB by user_id, then filter
        db_response = None
        try:
            db_response = table.query(
                IndexName='user_id_index',
                KeyConditionExpression=Key('user_id').eq(user_id)
            )
        except ClientError as e:
            print("Get annotation list from DynamoDB failed.", str(e))

        archive_jobs = []
        for annotation in db_response['Items']:
            if 'results_file_archive_id' in annotation \
                    and annotation['available_in_glacier'] \
                    and annotation['user_id'] == user_id:
                archive_jobs.append({'archive_id': annotation['results_file_archive_id'],
                                     'job_id': annotation['job_id'],
                                     's3_results_bucket': annotation['s3_results_bucket'],
                                     's3_key_result_file': annotation['s3_key_result_file']})

        # restore from Glacier
        # reference:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.initiate_job
        for archive_job in archive_jobs:
            try:
                expedited_response = glacier.initiate_job(
                    vaultName=GlacierVaultName,
                    jobParameters={
                        'Type': "archive-retrieval",
                        'ArchiveId': archive_job['archive_id'],
                        'SNSTopic': RestoreCompleteSNSArn,
                        'Tier': 'Expedited',
                        'Description': json.dumps(archive_job)
                    }
                )
            except:
                standard_response = glacier.initiate_job(
                    vaultName=GlacierVaultName,
                    jobParameters={
                        'Type': "archive-retrieval",
                        'ArchiveId': archive_job['archive_id'],
                        'SNSTopic': RestoreCompleteSNSArn,
                        'Tier': 'Standard',
                        'Description': json.dumps(archive_job)
                    }
                )

        # delete restore start SQS message
        helpers.delete_message_from_sqs(sqs, RestoreStartSQSURL, receipt_handle)


if __name__ == '__main__':
    main()
