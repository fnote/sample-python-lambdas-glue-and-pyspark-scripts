# Set up logging
import logging
import os
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
    if s3_object_key.startswith('new_customer'):
        input_file_name = s3_object_key.split('.')[0]  # remove file extensions
        folder_key = 'price_zone/new_customer_etl_output_' + input_file_name + '_' + etl_timestamp  # unique path prefix
        new_customer = True
        new_cstr_dpu_count_key = '/CP/' + env + '/ETL/REF_PRICE/DPU_COUNT/MIN'
        parameter = client_ssm.get_parameter(Name=new_cstr_dpu_count_key)['Parameter']
        if parameter['Name'] == new_cstr_dpu_count_key:
            glue_NumberOfWorkers = int(parameter['Value'])
    else:
        folder_key = 'price_zone/etl_output_' + etl_timestamp
        dpu_count_key = '/CP/' + env + '/ETL/REF_PRICE/DPU_COUNT/MAX'
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
        "etl_output_path_key": folder_key,
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
