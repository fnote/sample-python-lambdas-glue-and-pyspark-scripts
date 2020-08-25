# Set up logging
import logging
import os
import uuid
from urllib.parse import unquote_plus

import boto3
import json
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    client_step_function = boto3.client('stepfunctions')
    client_ssm = boto3.client('ssm')

    step_function_arn = os.environ['stepFunctionArn']
    env = os.environ['env']
    intermediate_s3_storage = os.environ['intermediateStorageS3']
    logger.info('Prize Zone lambda triggered by: ')
    logger.info(event)
    s3 = event['Records'][0]['s3']
    s3_object_key = unquote_plus(s3['object']['key'])
    s3_path = "s3://" + s3['bucket']['name'] + "/" + s3_object_key
    etl_timestamp = str(int(time.time()))
    new_customer = False

    glue_NumberOfWorkers = 0
    # here file name is not included to the path to prevent errors from filenames containing special character
    unique_path_prefix = 'etl_output_' + etl_timestamp + '_' \
                         + str(uuid.uuid4())  # generate unique Id to handle concurrent uploads
    if s3_object_key.startswith('customer'):  # handle new customer
        custom_path = 'new/' + unique_path_prefix
        folder_key = 'price_zone/' + custom_path
        new_customer = True
        new_cstr_dpu_count_key = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/DPU_COUNT/MIN'
        parameter = client_ssm.get_parameter(Name=new_cstr_dpu_count_key)['Parameter']
        if parameter['Name'] == new_cstr_dpu_count_key:
            glue_NumberOfWorkers = int(parameter['Value'])
    else:  # handle full load
        custom_path = 'all/' + unique_path_prefix
        folder_key = 'price_zone/' + custom_path
        dpu_count_key = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/DPU_COUNT/MAX'
        parameter = client_ssm.get_parameter(Name=dpu_count_key)['Parameter']
        if parameter['Name'] == dpu_count_key:
            glue_NumberOfWorkers = int(parameter['Value'])

    if glue_NumberOfWorkers == 0:
        error_msg = 'Received illegal value for glue_NumberOfWorkers: {}'.format(glue_NumberOfWorkers)
        raise ValueError(error_msg)

    intermediate_directory_path = "s3://" + intermediate_s3_storage + "/" + folder_key
    decompressed_file_path = intermediate_directory_path + "/decompress.csv"
    partitioned_files_key = folder_key + "/partitioned"
    partitioned_files_path = intermediate_directory_path + "/partitioned/"

    params = {
        "s3_path": s3_path,
        "intermediate_s3_name": intermediate_s3_storage,
        "partitioned_files_path": partitioned_files_path,
        "decompressed_file_path": decompressed_file_path,
        "partitioned_files_key": partitioned_files_key,
        "etl_timestamp": etl_timestamp,
        "etl_output_path_key": custom_path,
        "s3_input_bucket": s3['bucket']['name'],
        "s3_input_file_key": s3_object_key,
        "new_customer": new_customer,
        "dpu_count": glue_NumberOfWorkers
    }

    logger.info("Prize Zone data file Path: %s" % s3_path)
    logger.info("Prize Zone data intermediate s3 storage: %s" % intermediate_s3_storage)
    response = client_step_function.start_execution(
        stateMachineArn=step_function_arn,
        input=json.dumps(params)
    )
    logger.info('Started the Step Function: ' + step_function_arn)
    logger.info('Started at:' + str(response['startDate']))
    return {
        'arn': response['executionArn'],
        'startDate': str(response['startDate'])
    }
