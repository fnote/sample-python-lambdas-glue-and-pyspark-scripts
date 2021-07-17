# Set up logging
import json
import logging
import os
import time
from datetime import datetime
from urllib.parse import unquote_plus

# Import Boto 3 for AWS Glue
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# Define Lambda function
def lambda_handler(event, _):
    logger.info('## TRIGGERED BY EVENT: ')
    logger.info(event)
    step_function_arn = os.environ['stepFunctionArn']

    s3 = event['Records'][0]['s3']
    env = os.environ['env']
    s3_object_key = unquote_plus(s3['object']['key'])
    etl_timestamp = str(int(time.time()))
    folder_key = 'pa/etl_output_' + etl_timestamp
    s3_path = "s3://" + s3['bucket']['name'] + "/" + s3_object_key
    logger.info("File Path: %s" % s3_path)

    etl_time_object = datetime.fromtimestamp(int(etl_timestamp))

    archiving_path = 'pa/' + str(etl_time_object.year) + '/' + etl_time_object.strftime("%B") + '/' + str(
        etl_time_object.day) + '/etl_output_' + etl_timestamp + '/'

    step_function_input_params = {
        "s3_path": s3_path,
        "etl_timestamp": etl_timestamp,
        "intermediate_directory_path": folder_key,
        "etl_output_path_key": folder_key,
        "s3_input_bucket": s3['bucket']['name'],
        "s3_input_file_key": s3_object_key,
        "backup_bucket": 'cp-ref-etl-data-backup-storage-{}'.format(env.lower()),
        "backup_file_path": archiving_path,
        "env": env,
    }
    client = boto3.client('stepfunctions')
    client.start_execution(stateMachineArn=step_function_arn,
                                      input=json.dumps(step_function_input_params))

    logger.info('Started the Step Function: ' + step_function_arn)
