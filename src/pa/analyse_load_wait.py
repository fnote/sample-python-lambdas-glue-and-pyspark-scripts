# Set up logging
import json
import logging
import os
from datetime import datetime
# Import Boto 3 for AWS Glue
import boto3
import time
from urllib.parse import unquote_plus

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_values_from_ssm(keys):
    client_ssm = boto3.client('ssm')
    response = client_ssm.get_parameters(Names=keys)
    parameters = response['Parameters']
    invalidParameters = response['InvalidParameters']

    if invalidParameters:
        raise KeyError('Found invalid ssm parameter keys:' + ','.join(invalidParameters))

    parameter_dictionary = {}
    for parameter in parameters:
        parameter_dictionary[parameter['Name']] = parameter['Value']

    return parameter_dictionary


def get_execution_counts(env):
    max_allowed_concurrent_executions = '/CP/' + env + '/ETL/REF_PRICE/PA/MAX_ALLOWED_CONCURRENT_EXECUTIONS'

    ssm_keys = [max_allowed_concurrent_executions]
    ssm_key_values = get_values_from_ssm(ssm_keys)
    print(ssm_key_values)
    return ssm_key_values[max_allowed_concurrent_executions]


# Define Lambda function
def lambda_handler(event, context):
    logger.info('## TRIGGERED BY EVENT: ')
    logger.info(event)
    step_function_arn = os.environ['stepFunctionArn']
    environment = os.environ['env']

    max_allowed_concurrent_executions = get_execution_counts(environment)
    print(max_allowed_concurrent_executions)

    logger.info('Max allowed concurrent executions: ' + max_allowed_concurrent_executions)

    client = boto3.client('stepfunctions')
    response = client.list_executions(
        stateMachineArn = step_function_arn,
        statusFilter='RUNNING',
        maxResults=int(max_allowed_concurrent_executions) + 1
    )

    print(response)
    print(response['executions'])
    print(len(response['executions']))
    currently_running_execution_count = len(response['executions'])

    logger.info('Currently running execution count: ' + str(currently_running_execution_count))

    if int(currently_running_execution_count) < int(max_allowed_concurrent_executions):
        return {
            "shouldWait": False
        }
    else:
        return {
            "shouldWait": True
        }
