import json
import os
import time
from urllib.parse import unquote_plus
import logging

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def start_etl_flow(s3_event):
    step_function_arn = os.environ['stepFunctionArn']
    intermediate_s3_storage = os.environ['intermediateStorageS3']
    client = boto3.client('stepfunctions')
    logger.info('Prize Zone lambda triggered by: ')
    logger.info(s3_event)
    s3 = s3_event['Records'][0]['s3']
    s3_object_key = unquote_plus(s3['object']['key'])
    s3_path = "s3://" + s3['bucket']['name'] + "/" + s3_object_key
    etl_timestamp = str(int(time.time()))
    folder_key = 'price_zone/etl_output_' + etl_timestamp
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
        "s3_input_file_key": s3_object_key
    }

    logger.info("Prize Zone data file Path: %s" % s3_path)
    logger.info("Prize Zone data intermediate s3 storage: %s" % intermediate_s3_storage)
    response = client.start_execution(
        stateMachineArn=step_function_arn,
        input=json.dumps(params)
    )
    logger.info('Started the Step Function: ' + step_function_arn)
    logger.info('Started at:' + str(response['startDate']))
    return response


def delete_event_message_from_sqs(sqs, queue_url, receipt_handle, messageId):
    # log message ID
    sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle
    )
    logger.info("Deleted s3 event message with messageId " + messageId)


def lambda_handler():
    all_messages = []
    sqs = boto3.client('sqs')
    step_function = boto3.client('stepfunctions')
    url = 'https://sqs.us-east-1.amazonaws.com/037295147636/CP-REF-Price-Zone-New-Customer-Queue'

    MAX_NEW_PRICE_ZONE_ETLS_ALLOWED = 10
    step_function_execution_list = []

    execution_response = step_function.list_executions(
        stateMachineArn='arn:aws:states:us-east-1:037295147636:stateMachine:CP-REF-etl-prize-zone-state-machine-DEV',
        statusFilter='SUCCEEDED'
    )
    print(execution_response)
    for x in execution_response['executions']:
        print(x)

    # for msg_receive_count in range(0, MAX_NEW_PRICE_ZONE_ETLS_ALLOWED):
    #     response = sqs.receive_message(QueueUrl=url,
    #                                    WaitTimeSeconds=20,
    #                                    VisibilityTimeout=60,
    #                                    MaxNumberOfMessages=10)
    #
    #     print(response)
    #     if 'Messages' in response:
    #         for message in response['Messages']:
    #             logger.info("Received s3 event msg: " + message)
    #             messageId = message['MessageId']
    #             execution_response = step_function.list_executions(
    #                 stateMachineArn='arn:aws:states:us-east-1:037295147636:stateMachine:CP-REF-etl-prize-zone-state-machine-DEV',
    #                 statusFilter='RUNNING'
    #             )
    #             if len(execution_response['executions']) != MAX_NEW_PRICE_ZONE_ETLS_ALLOWED:
    #                 logger.info("Starting new ETL execution for messageId " + messageId)
    #                 etl_response = start_etl_flow(message['Body'])
    #                 step_function_execution_list.append(etl_response)
    #                 logger.info("Started new ETL execution for messageId " + messageId)
    #                 delete_event_message_from_sqs(sqs, url, message['ReceiptHandle'], messageId)
    #     else:
    #         print("No messages received from SQS")
    # print(all_messages)
    return {
        'statusCode': 200,
        'body': start_etl_flow
    }


lambda_handler()
