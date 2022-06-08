# views.py
#
# Copyright (C) 2011-2020 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import json
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Key
from botocore.client import Config
from botocore.exceptions import ClientError

from flask import (abort, flash, redirect, render_template,
                   request, session, url_for, Response)

from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile


"""
Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document.
"""


# ========================== Helper Functions ==========================

def errortmp(self_defined_message, error_message=None):
    """Template for error message"""
    if error_message:
        message = self_defined_message + ' ' + str(error_message)
    else:
        message = self_defined_message
    app.logger.error(message)
    return Response(response=json.dumps({'code': '500',
                                         'status': 'error',
                                         'message': message}),
                    status=500,
                    mimetype='application/json')


def get_s3():
    """Get S3 client"""
    try:
        s3 = boto3.client('s3',
                          region_name=app.config['AWS_REGION_NAME'],
                          config=Config(signature_version='s3v4'))
    except ClientError as e:
        return errortmp("Connect to S3 failed.", e)

    return s3


def get_dynamodb_table():
    """Get DynamoDB table"""
    try:
        dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
        table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    except ClientError as e:
        return errortmp("Connect to DynamoDB failed.", e)

    return table


def get_job_info_from_dynamodb(job_id):
    """Get job info from DynamoDB by job_id"""
    try:
        table = get_dynamodb_table()
        db_response = table.query(KeyConditionExpression=Key('job_id').eq(job_id))
    except ClientError as e:
        return errortmp("Get job info by job id from DynamoDB failed.", e)

    # check if invalid job_id
    if 'Items' not in db_response or not db_response['Items']:
        return errortmp("Invalid job id.")

    # get current job information
    curr_job = db_response['Items'][0]

    # check if job id belongs to the current user
    if curr_job['user_id'] != session['primary_identity']:
        app.logger.error(f"Not authorized to view this job")
        return abort(403)

    return curr_job


# ========================== Web Server Routes ==========================

@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
    # Create a session client to the S3 service
    s3 = get_s3()

    bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
    user_id = session['primary_identity']

    # Generate unique ID to be used as S3 key (name)
    key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
               str(uuid.uuid4()) + '~${filename}'

    # Create the redirect URL
    redirect_url = str(request.url) + '/job'

    # Define policy fields/conditions
    encryption = app.config['AWS_S3_ENCRYPTION']
    acl = app.config['AWS_S3_ACL']
    fields = {
        "success_action_redirect": redirect_url,
        "x-amz-server-side-encryption": encryption,
        "acl": acl
    }
    conditions = [
        ["starts-with", "$success_action_redirect", redirect_url],
        {"x-amz-server-side-encryption": encryption},
        {"acl": acl}
    ]

    # Generate the presigned POST call
    # reference:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.generate_presigned_post
    try:
        presigned_post = s3.generate_presigned_post(
            Bucket=bucket_name,
            Key=key_name,
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
    except ClientError as e:
        app.logger.error(f"Unable to generate presigned URL for upload: {e}")
        return abort(500)

    # Render the upload form which will parse/submit the presigned POST
    return render_template('annotate.html', s3_post=presigned_post)


"""
Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""


@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():
    # Get bucket name, key, and job ID from the S3 redirect URL
    submit_time = int(time.time())
    bucket_name = str(request.args.get('bucket'))
    s3_key = str(request.args.get('key'))

    # Extract the job ID from the S3 key
    _, user_id, full_filename = s3_key.split("/")
    job_id, input_file_name = full_filename.split("~")

    # Persist job to dynamo db
    # gather job information (to put to db)
    job_data = {
        'job_id': job_id,
        'user_id': user_id,
        'input_file_name': input_file_name,
        's3_inputs_bucket': bucket_name,
        's3_key_input_file': s3_key,
        'submit_time': submit_time,
        'job_status': 'PENDING'
    }

    # reference:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.put_item
    try:
        table = get_dynamodb_table()
        table.put_item(Item=job_data)
    except ClientError as e:
        return errortmp("Persist job info to DynamoDB failed.", e)

    # send message to request queue
    # https://docs.aws.amazon.com/sns/latest/api/API_Publish.html
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html#SNS.Client.publish
    try:
        sns = boto3.client('sns', region_name=app.config['AWS_REGION_NAME'])
        sns.publish(TopicArn=app.config['AWS_SNS_JOB_REQUEST_TOPIC'],
                    Message=json.dumps({'default': json.dumps(job_data)}),
                    MessageStructure='json')
    except ClientError as e:
        return errortmp("Publish notification message failed.", e)

    return render_template('annotate_confirm.html', job_id=job_id)


"""List all annotations for the user"""


@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():
    # Get list of annotations to display
    user_id = session['primary_identity']
    # reference:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.query
    try:
        table = get_dynamodb_table()
        db_response = table.query(
            IndexName='user_id_index',
            KeyConditionExpression=Key('user_id').eq(user_id)
        )
    except ClientError as e:
        return errortmp("Get annotation list failed.", e)

    annotations = db_response['Items']
    # change int type submit_time back to timestamp
    for annotation in annotations:
        annotation['submit_time'] = datetime.utcfromtimestamp(annotation['submit_time'])

    return render_template('annotations.html', annotations=annotations)


"""Display details of a specific annotation job"""


@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
    curr_job = get_job_info_from_dynamodb(id)

    # translate submit_time into human-readable format
    curr_job['submit_time'] = datetime.utcfromtimestamp(curr_job['submit_time'])

    # get input file download url
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.generate_presigned_url
    try:
        s3 = get_s3()
        input_file_download_url = s3.generate_presigned_url('get_object',
                                                            Params={
                                                                'Bucket': app.config['AWS_S3_INPUTS_BUCKET'],
                                                                'Key': curr_job['s3_key_input_file']
                                                            },
                                                            ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION']
                                                            )
    except ClientError as e:
        return errortmp("Get input file download url failed.", e)
    curr_job['input_file_url'] = input_file_download_url

    free_access_expired = False

    # if the job is completed
    if curr_job['job_status'] == 'COMPLETED':
        # translate complete_time into human-readable format
        curr_job['complete_time'] = datetime.utcfromtimestamp(curr_job['complete_time'])

        user_type = get_profile(session['primary_identity']).role

        # free user whose results files are in Glacier (=5 minutes after job completion)
        if user_type == 'free_user' and 'available_in_glacier' in curr_job and curr_job['available_in_glacier']:
            free_access_expired = True

        # premium user whose results files haven't finished restoring
        if user_type == 'premium_user' and 'available_in_glacier' in curr_job and curr_job['available_in_glacier']:
            curr_job['restore_message'] = 'The results file is restoring. Please wait.'

        # get result file download url
        # reference:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.generate_presigned_url
        try:
            result_file_download_url = s3.generate_presigned_url('get_object',
                                                                 Params={
                                                                     'Bucket': app.config['AWS_S3_RESULTS_BUCKET'],
                                                                     'Key': curr_job['s3_key_result_file']
                                                                 },
                                                                 ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION']
                                                                 )
        except ClientError as e:
            return errortmp("Get result file download url failed.", e)
        curr_job['result_file_url'] = result_file_download_url

    return render_template('annotation_details.html',
                           annotation=curr_job,
                           free_access_expired=free_access_expired)


"""Display the log file contents for an annotation job"""


@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
    curr_job = get_job_info_from_dynamodb(id)
    # reference:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object
    try:
        s3 = get_s3()
        s3_response = s3.get_object(Bucket=app.config['AWS_S3_RESULTS_BUCKET'],
                                    Key=curr_job['s3_key_log_file'])
        log_file_contents = s3_response['Body'].read().decode()
    except ClientError as e:
        return errortmp("Get job log from S3 failed.", e)

    return render_template('view_log.html', job_id=id, log_file_contents=log_file_contents)


"""Subscription management handler"""


@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
    if request.method == 'GET':
        # Display form to get subscriber credit card info
        if session.get('role') == "free_user":
            return render_template('subscribe.html')
        else:
            return redirect(url_for('profile'))

    elif request.method == 'POST':
        # Update user role to allow access to paid features
        update_profile(
            identity_id=session['primary_identity'],
            role="premium_user"
        )

        # Update role in the session
        session['role'] = "premium_user"

        # Request restoration of the user's data from Glacier
        # reference:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html#SNS.Client.publish
        restore_start_notification = {'user_id': session['primary_identity']}
        try:
            sns_restore_start = boto3.client('sns', region_name=app.config['AWS_REGION_NAME'])
            sns_restore_start.publish(TopicArn=app.config['AWS_SNS_RESTORE_START_TOPIC'],
                                      Message=json.dumps({'default': json.dumps(restore_start_notification)}),
                                      MessageStructure='json')
        except ClientError as e:
            return errortmp("Publish restore start notification failed.", e)

        # Display confirmation page
        return render_template('subscribe_confirm.html')


"""Reset subscription"""


@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
    # Hacky way to reset the user's role to a free user; simplifies testing
    update_profile(
        identity_id=session['primary_identity'],
        role="free_user"
    )
    return redirect(url_for('profile'))


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""


@app.route('/', methods=['GET'])
def home():
    return render_template('home.html')


"""Login page; send user to Globus Auth
"""


@app.route('/login', methods=['GET'])
def login():
    app.logger.info(f"Login attempted from IP {request.remote_addr}")
    # If user requested a specific page, save it session for redirect after auth
    if (request.args.get('next')):
        session['next'] = request.args.get('next')
    return redirect(url_for('authcallback'))


"""404 error handler
"""


@app.errorhandler(404)
def page_not_found(e):
    return render_template('error.html',
                           title='Page not found', alert_level='warning',
                           message="The page you tried to reach does not exist. \
      Please check the URL and try again."
                           ), 404


"""403 error handler
"""


@app.errorhandler(403)
def forbidden(e):
    return render_template('error.html',
                           title='Not authorized', alert_level='danger',
                           message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party."
                           ), 403


"""405 error handler
"""


@app.errorhandler(405)
def not_allowed(e):
    return render_template('error.html',
                           title='Not allowed', alert_level='warning',
                           message="You attempted an operation that's not allowed; \
      get your act together, hacker!"
                           ), 405


"""500 error handler
"""


@app.errorhandler(500)
def internal_error(error):
    return render_template('error.html',
                           title='Server error', alert_level='danger',
                           message="The server encountered an error and could \
      not process your request."
                           ), 500

### EOF
