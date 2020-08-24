import logging
import os
from collections import Counter

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    print("Received event:")
    print(event)
    step_function = boto3.client('stepfunctions')
    client_ssm = boto3.client('ssm')

    step_functionArn = event['stepFunctionArn']
    step_function_execution_id = event['stepFunctionExecutionId']
    env = os.environ['env']
    max_concurrency_ssm_key = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/MAX_CONCURRENCY'
    ALLOWED_CONCURRENT_EXECUTIONS = 0
    parameter = client_ssm.get_parameter(Name=max_concurrency_ssm_key)['Parameter']
    if parameter['Name'] == max_concurrency_ssm_key:
        ALLOWED_CONCURRENT_EXECUTIONS = int(parameter['Value'])

    if ALLOWED_CONCURRENT_EXECUTIONS == 0:
        error_msg = 'Received illegal value for Price Zone ETL workFlow maximum concurrency: {}'\
            .format(ALLOWED_CONCURRENT_EXECUTIONS)
        raise ValueError(error_msg)

    status = 'RUNNING'
    paginator = step_function.get_paginator('list_executions')
    pages = paginator.paginate(stateMachineArn=step_functionArn, statusFilter=status)
    print('Retrieved execution list for arn:%s with execution status:%s' % (step_functionArn, status))

    execution_dictionary = {}
    for page in pages:
        print(page)
        for execution in page['executions']:
            execution_arn = execution['executionArn']
            execution_dictionary[execution_arn] =  execution['startDate'].timestamp()

    sorted_start_time_list = sorted(list(execution_dictionary.values()))

    shouldWait = True

    if step_function_execution_id in execution_dictionary:
        step_function_start_time = execution_dictionary.get(step_function_execution_id)
        step_function_wait_index = sorted_start_time_list.index(step_function_start_time) + 1
        print("Execution index: %d for step function execution Id:%s with start time:%6f "
              % (step_function_wait_index, step_function_execution_id, step_function_start_time))

        shouldWait = True

        if step_function_wait_index <= ALLOWED_CONCURRENT_EXECUTIONS:
            start_time_counter = Counter(sorted_start_time_list)
            if start_time_counter[step_function_start_time] != 1:  # contains duplicates for same start time
                print("contains multiple execution ids with same start time: %.6f and number of entries %d"
                      % (step_function_start_time, start_time_counter[step_function_start_time]))
                execution_id_list = []
                for key, value in execution_dictionary.items():
                    if value == step_function_start_time:
                        execution_id_list.append(key)

                # sort from execution Id
                sorted_execution_id_list = sorted(execution_id_list)
                print("Execution ids containing same start time: ")
                print(sorted_execution_id_list)
                step_function_wait_index = step_function_wait_index + sorted_execution_id_list.index(
                    step_function_execution_id)
                if step_function_wait_index <= ALLOWED_CONCURRENT_EXECUTIONS:
                    print("New Execution index:%d for execution id:%s. Hence can proceed"
                          % (step_function_wait_index, step_function_execution_id))
                    shouldWait = False

            else:
                print("Does not contain duplicate start time values for %.6f. Hence execution id:%s "
                      "can proceed " % (step_function_start_time, step_function_execution_id))
                shouldWait = False
        else:
            print("Step function execution id:%s with start time:%.6f has to wait"
                  % (step_function_execution_id, step_function_start_time))

    return {
        'shouldWait': shouldWait
    }


def stop_execution():
    step_function = boto3.client('stepfunctions')
    status = 'RUNNING'
    paginator = step_function.get_paginator('list_executions')
    pages = paginator.paginate(
        stateMachineArn='arn:aws:states:us-east-1:037295147636:stateMachine:CP-REF-etl-prize-zone-state-machine-DEV',
        statusFilter=status)

    for page in pages:
        print(page)
        for execution in page['executions']:
            response = step_function.stop_execution(executionArn=execution['executionArn'])

# stop_execution()
