import requests
import time
import os
import logging
import boto3
import json
import anticrlf
import pymysql
import urllib.request
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

JOB_EXECUTION_STATUS_UPDATE_QUERY = 'UPDATE LOAD_JOB_EXECUTION_STATUS SET FAILED_OPCO_IDS = "{}", TOTAL_RECORD_COUNT = "{}",INVALID_RECORD_COUNT = "{}" WHERE FILE_NAME="{}" AND ETL_TIMESTAMP={}'
JOB_EXECUTION_STATUS_UPDATE_QUERY_WHEN_FAIL = 'UPDATE LOAD_JOB_EXECUTION_STATUS SET STATUS = "{}" ,END_TIME = "{}" WHERE FILE_NAME="{}" AND ETL_TIMESTAMP={}'

# Using a handler with anticrlf log formatter to avoid CRLF injections
# https://www.veracode.com/blog/secure-development/fixing-crlf-injection-logging-issues-python

charset = 'utf8'
cursor_type = pymysql.cursors.DictCursor


def get_additional_info(additional_info_details):
    additional_info_json = json.loads(additional_info_details)
    total_record_count = additional_info_json['received_records_count']
    received_valid_records_count = additional_info_json['received_valid_records_count']
    failed_opcos = additional_info_json['failed_opcos']
    failed_opcos_count = len(failed_opcos)
    failed_opco_list_string = ",".join(failed_opcos)
    invalid_record_count = total_record_count - received_valid_records_count

    return {
        "total_record_count": total_record_count,
        "received_valid_records_count": received_valid_records_count,
        "failed_opcos": failed_opcos,
        "failed_opcos_count": failed_opcos_count,
        "failed_opco_list_string": failed_opco_list_string,
        "invalid_record_count": invalid_record_count
    }

def get_values_from_ssm(keys):
    client_ssm = boto3.client('ssm')
    response = client_ssm.get_parameters(Names=keys, WithDecryption=True)
    # print(response)
    parameters = response['Parameters']
    invalid_parameters = response['InvalidParameters']
    if invalid_parameters:
        raise KeyError('Found invalid ssm parameter keys:' + ','.join(invalid_parameters))
    parameter_dictionary = {}
    for parameter in parameters:
        parameter_dictionary[parameter['Name']] = parameter['Value']
    print(parameter_dictionary)
    return parameter_dictionary


def get_connection_details(env):
    db_url = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/COMMON/DB_URL'
    password = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/COMMON/PASSWORD'
    username = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/COMMON/USERNAME'
    db_name = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/COMMON/DB_NAME'
    ssm_keys = [db_url, db_name, username, password]
    ssm_key_values = get_values_from_ssm(ssm_keys)
    print(ssm_key_values)
    return {
        "db_endpoint": ssm_key_values[db_url],
        "password": ssm_key_values[password],
        "username": ssm_key_values[username],
        "db_name": ssm_key_values[db_name]
    }


def get_db_connection(env):
    connection_params = get_connection_details(env)
    return pymysql.connect(
        host=connection_params['db_endpoint'], user=connection_params['username'], password=connection_params['password'], db=connection_params['db_name'], charset=charset, cursorclass=cursor_type)


formatter = anticrlf.LogFormatter('[%(levelname)s]\t%(asctime)s.%(msecs)dZ\t%(aws_request_id)s\t%(message)s\n', '%Y-%m-%dT%H:%M:%S')

for handler in logger.handlers:
    handler.setFormatter(formatter)

def read_additional_info(bucket_name, backup_completed, event):
    logger.info('bucket name: %s' % (bucket_name))
    if backup_completed:
        file_path = event['backup_file_path']
    else:
        file_path = event['etl_output_path_key'] + '/'

    s3_path = '{}additionalInfo.txt'.format(file_path)
    s3_client = boto3.client('s3')
    logger.info('s3 path: %s , bucket name : %s' % (s3_path, bucket_name))
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_path)
        additional_info = response['Body'].read().decode('utf-8')
        logger.info('Additional Info: {}'.format(additional_info))
        return additional_info
    except s3_client.exceptions.NoSuchKey:
        return 'None'

def lambda_handler(event, context):
    logger.info("Event: " + str(event))
    REFERENCE_PRICING = "REFERENCE_PRICING"

    url = os.environ['cp_notification_url']
    host = os.environ['cp_notification_host']
    env = os.environ['env']
    notification_event = event.get("event", "PROCESSOR")
    status = event.get("status", "ERROR")
    message = event.get("message", "NA")
    ref_price_type = event.get("event", "NA")
    bucket_name = event['additional_info_file_s3']


    current_time = int(time.time())
    logger.info('Sending notification env: %s, time: %s, status: %s, message: %s' % (
        env, current_time, status, message))
    additional_info = read_additional_info(bucket_name, status == 'SUCCEEDED', event)

    logger.info('Sending notification env: %s, time: %s, status: %s, message: %s' % (
        env, current_time, status, message))

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

    print(additional_info)

    # add record count to common db status table if event is prize zone
    # file name and etl time stamp required to edit the right record in db

    if notification_event == "[ETL] - [Ref Price] [Price Zone Data]" and status == "SUCCEEDED":
        additional_info_json = json.loads(additional_info)
        total_record_count = additional_info_json['received_records_count']
        received_valid_records_count = additional_info_json['received_valid_records_count']
        failed_opcos = additional_info_json['failed_opcos']
        failed_opco_list_string = ",".join(failed_opcos)
        invalid_record_count = total_record_count - received_valid_records_count

        etl_timestamp = event['etl_timestamp']
        input_file_name = event['file_name']

        logger.info('updating status DB with file name: %s, etl timestamp: %s, env: %s, failed opcos: %s, invalid record count:%s' % (
            input_file_name, etl_timestamp, env, failed_opco_list_string, invalid_record_count))
        database_connection = get_db_connection(env)
        cursor_object = database_connection.cursor()
        # add failed opcos here and increment failed opcos count
        cursor_object.execute(JOB_EXECUTION_STATUS_UPDATE_QUERY.format(failed_opco_list_string, str(total_record_count), str(invalid_record_count), input_file_name, etl_timestamp))
        database_connection.commit()

    if notification_event in ["ETL-PRICE_ZONE-OUTSIDE-FAILURE", "ETL-PA"] and status == "ERROR":
        # we do not know whether additional info file got created
        logger.info('file has failed before map state , update the execution status table with failed')
        print(additional_info)
        etl_timestamp = event['etl_timestamp']
        input_file_name = event['file_name']
        end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        logger.info('updating status DB with file name: %s, etl timestamp: %s, env: %s' % (
            input_file_name, etl_timestamp, env))
        database_connection = get_db_connection(env)
        cursor_object = database_connection.cursor()
        cursor_object.execute(
            JOB_EXECUTION_STATUS_UPDATE_QUERY_WHEN_FAIL.format("FAILED", end_time, input_file_name, etl_timestamp))
        database_connection.commit()

    if notification_event == "[ETL] - [Ref Price] [Price Data]":
        etl_timestamp = event['etl_timestamp']
        input_file_name = event['file_name']
        logger.info('PA File successful , update executions status table')
        database_connection = get_db_connection(env)
        cursor_object = database_connection.cursor()
        end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cursor_object.execute(
            JOB_EXECUTION_STATUS_UPDATE_QUERY_WHEN_FAIL.format("SUCCEEDED", end_time, input_file_name, etl_timestamp))
        database_connection.commit()


        # Teams alerts for failed files
    if status == 'ERROR':
            print("failed opcos present , send teams alert")

            try:
                teams_url = 'teams_webhook_url_' + env
                teams_webhook_url = os.environ[teams_url]

                payload = {
                    "text": json.dumps(data['message'])
                }
                requests.post(teams_webhook_url, data=json.dumps(payload))
            except Exception as e:
                logger.error(e)

    # update the status table with total record count
    headers = {'host': host, 'Content-Type': 'application/json'}
    response = requests.post(url, json=data, headers=headers)
    logger.info('Response: %s' % response.json())
    return response.json()
