import logging
import os
from collections import Counter

import boto3
from botocore.config import Config

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# adaptive - Retries with additional client side throttling.
config = Config(
   retries={
      'max_attempts': 50,
      'mode': 'adaptive'
   }
)


def get_values_from_ssm(keys):
    client_ssm = boto3.client('ssm', config=config)
    response = client_ssm.get_parameters(Names=keys, WithDecryption=True)
    parameters = response['Parameters']
    invalid_parameters = response['InvalidParameters']
    if invalid_parameters:
        raise KeyError('Found invalid ssm parameter keys:' + ','.join(invalid_parameters))
    parameter_dictionary = {}
    for parameter in parameters:
        parameter_dictionary[parameter['Name']] = parameter['Value']
    print(parameter_dictionary)
    return parameter_dictionary


def get_max_concurrency(env):
    max_concurrency_ssm_key = '/CP/' + env + '/ETL/REF_PRICE/PA/MAX_ALLOWED_CONCURRENT_EXECUTIONS'
    ssm_keys = [max_concurrency_ssm_key]
    ssm_key_values = get_values_from_ssm(ssm_keys)
    logger.info('GetParameter called for ssm key: %s' % max_concurrency_ssm_key)
    return {
        "max_concurrency": ssm_key_values[max_concurrency_ssm_key]
    }


def lambda_handler(event, context):
    logger.info("Received event:")
    logger.info(event)
    step_function = boto3.client('stepfunctions', config=config)

    step_functionArn = event['stepFunctionArn']
    step_function_execution_id = event['stepFunctionExecutionId']
    env = os.environ['env']

    params = get_max_concurrency(env)
    ALLOWED_CONCURRENT_EXECUTIONS = int(params['max_concurrency'])

    if ALLOWED_CONCURRENT_EXECUTIONS == 0:
        error_msg = 'Received illegal value for PA ETL workFlow maximum concurrency: {}' \
            .format(ALLOWED_CONCURRENT_EXECUTIONS)
        raise ValueError(error_msg)

    status = 'RUNNING'
    paginator = step_function.get_paginator('list_executions')
    pages = paginator.paginate(stateMachineArn=step_functionArn, statusFilter=status)
    logger.info('paginator:%s  pages:%s' % (paginator, pages))
    logger.info('Retrieved execution list for step function:%s with execution status:%s' % (step_functionArn, status))

    execution_dictionary = {}
    for page in pages:
        logger.info(page)
        for execution in page['executions']:
            execution_arn = execution['executionArn']
            execution_dictionary[execution_arn] = execution['startDate'].timestamp()

    sorted_start_time_list = sorted(list(execution_dictionary.values()))

    shouldWait = True

    if step_function_execution_id in execution_dictionary:
        step_function_start_time = execution_dictionary.get(step_function_execution_id)
        step_function_wait_index = sorted_start_time_list.index(step_function_start_time) + 1
        logger.info("Current execution index: %d for  step function execution Id:%s with start time:%6f "
                    % (step_function_wait_index, step_function_execution_id, step_function_start_time))

        if step_function_wait_index <= ALLOWED_CONCURRENT_EXECUTIONS:
            start_time_counter = Counter(sorted_start_time_list)
            if start_time_counter[step_function_start_time] != 1:  # contains duplicates for same start time
                logger.info("contains multiple execution ids with same start time: %.6f and number of entries %d"
                            % (step_function_start_time, start_time_counter[step_function_start_time]))
                execution_id_list = []
                for key, value in execution_dictionary.items():
                    if value == step_function_start_time:
                        execution_id_list.append(key)

                # sort from execution Id
                sorted_execution_id_list = sorted(execution_id_list)
                logger.info("Execution ids containing same start time: ")
                logger.info(sorted_execution_id_list)
                step_function_wait_index = step_function_wait_index + sorted_execution_id_list.index(
                    step_function_execution_id)
                if step_function_wait_index <= ALLOWED_CONCURRENT_EXECUTIONS:
                    logger.info("New Execution index:%d for execution id:%s. Hence can proceed"
                                % (step_function_wait_index, step_function_execution_id))
                    shouldWait = False

            else:
                logger.info("Does not contain duplicate start time values for %.6f. Hence execution id:%s "
                            "with list index %d can proceed " % (step_function_start_time,
                                                                 step_function_execution_id,
                                                                 step_function_wait_index))
                shouldWait = False
        else:
            logger.info("Step function execution id:%s with start time:%.6f has list index %d. Allowed concurrency %d"
                        % (step_function_execution_id, step_function_start_time, step_function_wait_index,
                           ALLOWED_CONCURRENT_EXECUTIONS))
    else:
        logger.info("could not locate step function execution Id %s in the received execution list"
                    % step_function_execution_id)

    return {
        'shouldWait': shouldWait
    }
