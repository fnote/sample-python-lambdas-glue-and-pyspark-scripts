# Set up logging
import logging
import os
import boto3
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    step_function_arn = os.environ['stepFunctionArn']
    intermediate_s3_storage = os.environ['intermediateStorageS3']
    client =boto3.client('stepfunctions')
    logger.info('Prize Zone lambda triggered by: ')
    logger.info(event)
    s3 = event['Records'][0]['s3']
    s3_object_key = s3['object']['key']
    s3_path = "s3://" + s3['bucket']['name'] + "/" + s3_object_key
    intermediate_directory_path = "s3://" + intermediate_s3_storage + "/" + s3_object_key
    decompressed_file_path = intermediate_directory_path + "/decompress.csv"
    params = {
        "s3_path": s3_path,
        "intermediate_s3_name": intermediate_s3_storage,
        "intermediate_directory_path": intermediate_directory_path,
        "decompressed_file_path": decompressed_file_path
    }

    logger.info("Prize Zone data file Path: %s" % s3_path)
    logger.info("Prize Zone data intermediate s3 storage: %s" % intermediate_s3_storage)
    response = client.start_execution(
        stateMachineArn=step_function_arn,
        input=json.dumps(params)
    )
    logger.info('Started the Step Function: ' + step_function_arn)
    logger.info('Started at:' + str(response['startDate']))
    return {
        'arn': response['executionArn'],
        'startDate':  str(response['startDate'])
    }
