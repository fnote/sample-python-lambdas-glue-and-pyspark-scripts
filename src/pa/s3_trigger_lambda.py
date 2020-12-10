# Set up logging
import json
import logging
import os
# Import Boto 3 for AWS Glue
import boto3
import time
from urllib.parse import unquote_plus

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# Define Lambda function
def lambda_handler(event, context):
    logger.info('## TRIGGERED BY EVENT: ')
    logger.info(event)
    step_function_arn = os.environ['stepFunctionArn']

    s3 = event['Records'][0]['s3']
    s3_object_key = unquote_plus(s3['object']['key'])
    etl_timestamp = str(int(time.time()))
    folder_key = 'pa/etl_output_' + etl_timestamp
    s3_path = "s3://" + s3['bucket']['name'] + "/" + s3_object_key
    intermediate_directory_path = "/" + s3_object_key
    logger.info("File Path: %s" % s3_path)

    step_function_input_params = {
        "s3_path": s3_path,
        "etl_timestamp": etl_timestamp,
        "intermediate_directory_path": "/" + s3_object_key,
        "etl_output_path_key": folder_key,
        "s3_input_bucket": s3['bucket']['name'],
        "s3_input_file_key": s3_object_key
    }
    client = boto3.client('stepfunctions')
    response = client.start_execution(stateMachineArn=step_function_arn,
                                      input=json.dumps(step_function_input_params))

    logger.info('Started the Step Function: ' + step_function_arn)
