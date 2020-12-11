import requests
import time
import os
import logging
import boto3
import json
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def read_additional_info(bucket_name, backup_completed, event):
    if backup_completed:
        etl_timestamp = event['etl_timestamp']
        etl_output_path_key = event['etl_output_path_key']
        etl_time_object = datetime.fromtimestamp(int(etl_timestamp))

        file_path = 'price_zone/' + str(etl_time_object.year) + '/' + etl_time_object.strftime("%B") + '/'\
            + str(etl_time_object.day) + '/' + etl_output_path_key
    else:
        file_path = event['additional_info_file_key']

    s3_path = '{}/additionalInfo.txt'.format(file_path)
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_path)
        additional_info = response['Body'].read().decode('utf-8')
        logger.info('Additional Info: {}'.format(additional_info))
        return additional_info
    except s3_client.exceptions.NoSuchKey:
        return 'None'

def lambda_handler(event, context):
    REFERENCE_PRICING = "REFERENCE_PRICING"

    url = os.environ['cp_notification_url']
    host = os.environ['cp_notification_host']
    env = os.environ['env']
    notification_event = event.get("event", "PROCESSOR")
    status = event.get("status", "ERROR")
    message = event.get("message", "NA")
    bucket_name = event['additional_info_file_s3']
    current_time = int(time.time())
    logger.info('Sending notification env: %s, time: %s, status: %s, message: %s' % (
        env, current_time, status, message))
    additional_info = read_additional_info(bucket_name, status == 'SUCCEEDED', event)

    data = {
        "messageAttributes": {
            "application": REFERENCE_PRICING,
            "event": notification_event,
            "status": status,
            "environment": env,
            "triggeredTime": current_time,
        },
        "message": {
            "application": REFERENCE_PRICING,
            "event": notification_event,
            "status": status,
            "message": message,
            "triggeredTime": current_time,
            "additional_info": json.dumps(additional_info)
        }
    }

    headers = {'host': host, 'Content-Type': 'application/json'}
    response = requests.post(url, json=data, headers=headers)
    logger.info('Response: %s' % response.json())
    return response.json()
