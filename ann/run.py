# run.py
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
#
# Wrapper script for running AnnTools
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from configparser import SafeConfigParser

import sys
import time
import shutil
import os
import json

import driver
import sys
sys.path.insert(0, '/home/ec2-user/mpcs-cc/gas/util')
import helpers


config = SafeConfigParser(os.environ)
config.read('ann_config.ini')

AwsRegionName = config['aws']['AwsRegionName']
TableName = config['aws']['TableName']
ResultBucketName = config['aws']['S3ResultsBucket']
S3KeyPrefix = config['aws']['S3KeyPrefix']
ResultsSNSArn = config['aws']['ResultsSNSArn']

"""A rudimentary timer for coarse-grained profiling
"""


class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f"Approximate runtime: {self.secs:.2f} seconds")


if __name__ == '__main__':
    # Call the AnnTools pipeline
    if len(sys.argv) > 1:
        with Timer():
            driver.run(sys.argv[1], 'vcf')

        # upload the results and log file to S3
        # sys.argv[1] example:
        # jobs/fake_user/jobid/jobid~test_zhicongm.vcf
        full_filename = sys.argv[1]
        job_id = sys.argv[2]
        user_id = full_filename.split('/')[1]
        # job_id = full_filename.split('/')[2] + '/'
        filename = full_filename.split('/')[3]

        s3 = boto3.client('s3',
                          region_name=AwsRegionName,
                          config=Config(signature_version='s3v4')
                          )

        # reference:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html
        result_object_name = S3KeyPrefix + user_id + '/' + job_id + filename[:-3] + 'annot.vcf'
        log_object_name = S3KeyPrefix + user_id + '/' + job_id + filename + '.count.log'
        try:
            # upload result file
            s3.upload_file(full_filename[:-3] + 'annot.vcf',
                           ResultBucketName,
                           result_object_name)
            # upload log file
            s3.upload_file(full_filename + '.count.log',
                           ResultBucketName,
                           log_object_name)
        except ClientError as e:
            print(f"Upload result/log file failed. {str(e)}")

        # update job info to dynamo db
        # reference:
        # https://highlandsolutions.com/blog/hands-on-examples-for-working-with-dynamodb-boto3-and-python
        complete_time = int(time.time())
        try:
            db = boto3.resource('dynamodb', region_name=AwsRegionName)
            table = db.Table(TableName)
            table.update_item(
                Key={'job_id': job_id},
                UpdateExpression='set s3_results_bucket=:var_s3_results_bucket,\
                                    s3_key_result_file=:var_s3_key_result_file,\
                                    s3_key_log_file=:var_s3_key_log_file,\
                                    complete_time=:var_complete_time,\
                                    job_status=:var_job_status',
                ExpressionAttributeValues={
                    ':var_s3_results_bucket': ResultBucketName,
                    ':var_s3_key_result_file': result_object_name,
                    ':var_s3_key_log_file': log_object_name,
                    ':var_complete_time': complete_time,
                    ':var_job_status': 'COMPLETED'
                },
                ReturnValues='UPDATED_NEW'
            )
        except ClientError as e:
            print(f"Update finished job info to database failed. {str(e)}")

        # clean up load job files
        shutil.rmtree(f'jobs/{user_id}/{job_id}')

        # SNS: public notification to SNS (job_results) about job being done
        # https://docs.aws.amazon.com/sns/latest/api/API_Publish.html
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html#SNS.Client.publish
        user_email = helpers.get_user_profile(user_id)[0][2]

        job_completion_notification = {
            'job_id': job_id,
            'user_id': user_id,
            'user_email': user_email,
            's3_results_bucket': ResultBucketName,
            's3_key_result_file': result_object_name,
            's3_key_log_file': log_object_name,
            'complete_time': complete_time
        }

        try:
            sns = boto3.client('sns', region_name=AwsRegionName)
            sns.publish(TopicArn=ResultsSNSArn,
                        Message=json.dumps({'default': json.dumps(job_completion_notification)}),
                        MessageStructure='json')
        except ClientError as e:
            print(f"Publish job completion notification message failed. {str(e)}")

    else:
        print("A valid .vcf file must be provided as input to this program.")
